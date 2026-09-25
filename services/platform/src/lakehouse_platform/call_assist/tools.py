"""Tool access for the assist engine: the governed MCP gateway, called as the colleague.

The engine depends on the `ToolBackend` protocol, not on MCP, so evals can
substitute recorded fixtures and run with no platform at all.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import AsyncExitStack
from typing import Any, Protocol

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

log = logging.getLogger(__name__)


class ToolFailure(Exception):
    """kind: denied (policy said no), unavailable (platform down), error (anything else)."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class ToolBackend(Protocol):
    async def call(self, tool: str, args: dict[str, Any]) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class MCPTools:
    """One MCP client per call, carrying that colleague's bearer token (refreshable)."""

    def __init__(self, url: str, token: str, timeout_s: float = 9.0) -> None:
        self.url = url
        self.timeout_s = timeout_s
        self.http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {token}"}, timeout=timeout_s + 1)
        self._stack: AsyncExitStack | None = None
        self._client: Client | None = None
        self._lock = asyncio.Lock()

    def update_token(self, token: str) -> None:
        self.http.headers["Authorization"] = f"Bearer {token}"

    async def _ensure(self) -> Client:
        async with self._lock:
            if self._client is None:
                stack = AsyncExitStack()
                self._client = await stack.enter_async_context(
                    Client(streamable_http_client(self.url, http_client=self.http))
                )
                self._stack = stack
            return self._client

    async def call(self, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        try:
            client = await self._ensure()
            result = await asyncio.wait_for(client.call_tool(tool, args), self.timeout_s)
        except (TimeoutError, OSError, httpx2.HTTPError) as exc:
            await self._reset()
            raise ToolFailure("unavailable", f"gateway unreachable: {exc}") from exc
        if result.is_error:
            text = " ".join(getattr(c, "text", "") for c in result.content)
            kind = (
                "denied" if "denied" in text.lower() else "unavailable" if "unavailable" in text else "error"
            )
            raise ToolFailure(kind, text)
        return result.structured_content or json.loads(result.content[0].text)

    async def _reset(self) -> None:
        async with self._lock:
            if self._stack is not None:
                try:
                    await self._stack.aclose()
                except Exception as exc:  # noqa: BLE001 - best-effort teardown of a broken session
                    log.debug("mcp client teardown: %s", exc)
            self._stack, self._client = None, None

    async def close(self) -> None:
        await self._reset()
        await self.http.aclose()
