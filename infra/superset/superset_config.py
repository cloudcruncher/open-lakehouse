"""Superset: the SQL workbench for colleagues. Keycloak sign-in only, no local passwords.

The browser talks to Keycloak on localhost:8280; the container reaches it as keycloak:8080.
Tokens are issued for http://localhost:8280/realms/bank either way (same as Trino expects).
"""

import os

from flask_appbuilder.security.manager import AUTH_OAUTH
from superset.security import SupersetSecurityManager

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://superset:{os.environ['SUPERSET_DB_PASSWORD']}@postgres:5432/superset"

# Every portal is on localhost: cookies are shared across ports, so keep ours distinct.
SESSION_COOKIE_NAME = "superset_session"

BROWSER_REALM = "http://localhost:8280/realms/bank"
INTERNAL_REALM = "http://keycloak:8080/realms/bank"

AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [
    {
        # "keycloak" selects Flask-AppBuilder's built-in userinfo handling.
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": "superset",
            "client_secret": os.environ["SUPERSET_CLIENT_SECRET"],
            "api_base_url": f"{INTERNAL_REALM}/protocol/",
            "authorize_url": f"{BROWSER_REALM}/protocol/openid-connect/auth",
            "access_token_url": f"{INTERNAL_REALM}/protocol/openid-connect/token",
            "jwks_uri": f"{INTERNAL_REALM}/protocol/openid-connect/certs",
            "issuer": BROWSER_REALM,
            "client_kwargs": {
                "scope": "openid profile email",
                "code_challenge_method": "S256",
            },
        },
    }
]

# Who may administer Superset. Data access is not decided here: Trino and OPA decide that
# per colleague, whatever their Superset role. So every colleague may build (Alpha: register
# datasets, make charts and dashboards; tenant teams own their analytics, ADR 14) without that
# widening what any chart returns: a table a colleague may not read fails at Trino for them.
ADMINS = {"ops_admin"}


class BankSecurityManager(SupersetSecurityManager):
    def oauth_user_info(self, provider, response=None):
        info = self.get_oauth_user_info(
            provider, response
        )  # FAB's built-in Keycloak handling
        info["role_keys"] = ["admin" if info.get("username") in ADMINS else "colleague"]
        return info


CUSTOM_SECURITY_MANAGER = BankSecurityManager
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Gamma"
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLES_MAPPING = {"admin": ["Admin"], "colleague": ["Alpha", "sql_lab"]}


def FLASK_APP_MUTATOR(app):  # noqa: N802 - name set by Superset
    from flask import redirect, request

    # Keycloak only accepts localhost as the return address; a visit via 127.0.0.1 would
    # fail at sign-in with "Invalid parameter: redirect_uri".
    @app.before_request
    def canonical_host():
        if request.host.split(":")[0] == "127.0.0.1":
            return redirect(request.url.replace("//127.0.0.1", "//localhost", 1), code=308)

    # A colleague's saved Trino token dies with their Keycloak session (30 min idle).
    # Keycloak refuses the refresh with HTTP 400, which Superset treats as an unrelated
    # error: it keeps the dead token and every query fails with "400 Client Error". As
    # Trino's auth error instead, Superset deletes the token and asks to authorize again.
    import requests
    from superset.db_engine_specs.trino import TrinoAuthError, TrinoEngineSpec

    fresh_token = TrinoEngineSpec.get_oauth2_fresh_token.__func__

    def get_oauth2_fresh_token(cls, config, refresh_token):
        try:
            return fresh_token(cls, config, refresh_token)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 401):
                raise TrinoAuthError(
                    "error 401: saved sign-in expired, authorize again"
                ) from exc
            raise  # Keycloak down (5xx) is an outage, not an expired sign-in

    TrinoEngineSpec.get_oauth2_fresh_token = classmethod(get_oauth2_fresh_token)


# SQL Lab queries Trino with the colleague's own Keycloak token: Superset asks once per
# colleague ("Authorize"), stores and refreshes the token, and sends it on every query.
# Trino and OPA then apply that colleague's rules. There is no shared service account.
DATABASE_OAUTH2_CLIENTS = {
    "Trino": {
        "id": "superset",
        "secret": os.environ["SUPERSET_CLIENT_SECRET"],
        "scope": "openid",
        "authorization_request_uri": f"{BROWSER_REALM}/protocol/openid-connect/auth",
        "token_request_uri": f"{INTERNAL_REALM}/protocol/openid-connect/token",
    }
}
# Built while the query runs, where the request's port is not known; pin it to the
# address registered on the Keycloak client.
DATABASE_OAUTH2_REDIRECT_URI = "http://localhost:3004/api/v1/database/oauth2/"
