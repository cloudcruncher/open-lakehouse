import os

os.environ.setdefault("POLARIS_ROOT_CLIENT_SECRET", "test-only")

from lakehouse_platform.bootstrap import polaris as pb  # noqa: E402

ENGINE = pb.Engine("tenant_x", "tenant_x", "tenant_x_writer", "POLARIS")


class FakePolaris:
    """Records calls; the principal already exists and any stored credentials are stale."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def call(self, method, path, body=None, ok_if_exists=True):
        self.calls.append((method, path))
        if path.endswith("/reset"):
            return {"credentials": {"clientId": "new-id", "clientSecret": "new-secret"}}
        return {}  # 409 on create: already exists

    def credentials_work(self, client_id, client_secret):
        return False


def test_lost_credentials_are_reset_by_the_admin_not_rotated(tmp_path):
    p = FakePolaris()
    secret = tmp_path / "tenants/x/polaris.env"
    pb.ensure_principal(p, ENGINE, secret)
    paths = [path for _, path in p.calls]
    assert "/api/management/v1/principals/tenant_x/reset" in paths
    assert not any(path.endswith("/rotate") for path in paths)
    assert pb.read_env_file(secret) == {"POLARIS_CLIENT_ID": "new-id", "POLARIS_CLIENT_SECRET": "new-secret"}
