"""Identity for the gateway: verify the colleague's token, then exchange it.

The agent never holds a database credential. It presents the colleague's
Keycloak access token (audience `mcp-gateway`). The gateway validates it, then
performs an RFC 8693 token exchange for a token whose audience is `trino` and whose
subject is still the colleague. Trino authenticates that token and OPA applies the
colleague's own row filters and masks. The agent can never see more than the human
it is assisting.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass

import httpx
import jwt
from mcp.server.auth.provider import AccessToken, TokenVerifier

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class IdentityConfig:
    issuer: str  # e.g. http://localhost:8280/realms/bank (what tokens say)
    internal_base: str  # e.g. http://keycloak:8080/realms/bank (how we reach it)
    audience: str  # mcp-gateway
    client_id: str
    client_secret: str
    downstream_audience: str = "trino"

    @property
    def jwks_url(self) -> str:
        return f"{self.internal_base}/protocol/openid-connect/certs"

    @property
    def token_url(self) -> str:
        return f"{self.internal_base}/protocol/openid-connect/token"


class KeycloakTokenVerifier(TokenVerifier):
    """Validates signature (JWKS, cached with rotation), issuer, audience and expiry."""

    def __init__(self, cfg: IdentityConfig) -> None:
        self.cfg = cfg
        self.jwks = jwt.PyJWKClient(cfg.jwks_url, cache_keys=True, lifespan=300)

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            key = self.jwks.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                key.key,
                algorithms=["RS256"],
                audience=self.cfg.audience,
                issuer=self.cfg.issuer,
                options={"require": ["exp", "iat", "sub", "aud", "iss"]},
            )
        except jwt.PyJWTError as exc:
            log.info("rejected bearer token: %s", exc)
            return None
        return AccessToken(
            token=token,
            client_id=claims.get("azp", "unknown"),
            scopes=str(claims.get("scope", "")).split(),
            expires_at=claims["exp"],
            subject=claims["sub"],
            claims=claims,
        )


class TokenExchanger:
    """RFC 8693 token exchange with a small cache keyed by the subject token's hash."""

    def __init__(self, cfg: IdentityConfig) -> None:
        self.cfg = cfg
        self.http = httpx.Client(timeout=5.0)
        self._cache: dict[str, tuple[str, float]] = {}
        self._lock = threading.Lock()

    def exchange(self, subject_token: str) -> str:
        key = hashlib.sha256(subject_token.encode()).hexdigest()
        now = time.time()
        with self._lock:
            hit = self._cache.get(key)
            if hit and hit[1] - 30 > now:
                return hit[0]
        resp = self.http.post(
            self.cfg.token_url,
            auth=(self.cfg.client_id, self.cfg.client_secret),
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token": subject_token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "audience": self.cfg.downstream_audience,
            },
        )
        if resp.status_code != 200:
            raise PermissionError(f"token exchange refused ({resp.status_code}): {resp.text[:200]}")
        body = resp.json()
        token = body["access_token"]
        with self._lock:
            self._cache[key] = (token, now + float(body.get("expires_in", 60)))
            if len(self._cache) > 10_000:  # bounded memory; expired entries go first
                for k in [k for k, (_, exp) in self._cache.items() if exp < now]:
                    self._cache.pop(k, None)
        return token
