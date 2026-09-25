# /// script
# requires-python = ">=3.13"
# dependencies = ["mcp==2.2.0", "httpx>=0.28"]
# ///
"""Act as a colleague-assist agent: sign in as a colleague, then call the MCP tools.

Usage: uv run scripts/agent_call.py <colleague> <tool> '<json args>'
       uv run scripts/agent_call.py alice list
The password grant is for local demos only; in production the agent console uses
SSO (authorization code + PKCE) and passes the colleague's token through.
"""

import asyncio
import json
import pathlib
import sys

import httpx
import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

ROOT = pathlib.Path(__file__).resolve().parent.parent
ENV = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if "=" in line
)


def colleague_token(user: str) -> str:
    r = httpx.post(
        "http://localhost:8280/realms/bank/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "agent-console",
            "username": user,
            "password": ENV["DEMO_USER_PASSWORD"],
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


async def main(user: str, tool: str, args: dict) -> int:
    headers = {"Authorization": f"Bearer {colleague_token(user)}"}
    async with httpx2.AsyncClient(headers=headers, timeout=30) as http:
        async with Client(
            streamable_http_client("http://localhost:8000/mcp", http_client=http)
        ) as client:
            if tool == "list":
                for t in (await client.list_tools()).tools:
                    print(f"- {t.name}: {(t.description or '').splitlines()[0]}")
                return 0
            result = await client.call_tool(tool, args)
            if result.is_error:
                print(
                    "TOOL ERROR:",
                    " ".join(getattr(c, "text", "") for c in result.content),
                )
                return 2
            payload = result.structured_content or json.loads(result.content[0].text)
            print(json.dumps(payload, indent=2, default=str))
            return 0


if __name__ == "__main__":
    user, tool = sys.argv[1], sys.argv[2]
    sys.exit(
        asyncio.run(
            main(user, tool, json.loads(sys.argv[3]) if len(sys.argv) > 3 else {})
        )
    )
