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


class CatalogPolaris:
    """An existing catalog whose stored properties and storage config are whatever the test sets."""

    def __init__(self, properties: dict) -> None:
        self.puts: list[dict] = []
        self.properties = properties

    def exists(self, path):
        return True

    def call(self, method, path, body=None, ok_if_exists=True):
        if method == "PUT":
            self.puts.append(body)
            return {}
        return {
            "entityVersion": 3,
            "properties": self.properties,
            "storageConfigInfo": pb.desired_storage_config(),
        }


def test_catalog_allows_drop_with_purge_so_tenants_can_drop_what_they_create():
    assert pb.desired_properties()["polaris.config.drop-with-purge.enabled"] == "true"


def test_an_existing_catalog_without_the_property_is_reconciled_keeping_other_properties():
    p = CatalogPolaris({"default-base-location": "s3://b/warehouse", "custom": "kept"})
    pb.ensure_catalog(p)
    assert len(p.puts) == 1
    assert p.puts[0]["currentEntityVersion"] == 3
    assert p.puts[0]["properties"]["polaris.config.drop-with-purge.enabled"] == "true"
    assert p.puts[0]["properties"]["custom"] == "kept"


def test_a_catalog_that_already_matches_is_left_alone():
    p = CatalogPolaris(pb.desired_properties())
    pb.ensure_catalog(p)
    assert p.puts == []
