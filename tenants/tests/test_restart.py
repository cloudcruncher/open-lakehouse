import pytest
import restart

TENANTS = {
    "markets-data": {
        "name": "markets-data",
        "codeLocation": {"deploy": True},
        "services": [{"name": "coinbase-feed", "deploy": True}, {"name": "idle", "deploy": False}],
    },
    "nocode": {"name": "nocode", "codeLocation": {"deploy": False}},
}


def test_a_declared_service_and_the_code_server_resolve():
    assert restart.target(TENANTS, "markets-data", "coinbase-feed") == "tenant-markets-data-coinbase-feed"
    assert restart.target(TENANTS, "markets-data", "code") == "tenant-markets-data-code"


@pytest.mark.parametrize(
    ("tenant", "service", "message"),
    [
        ("nobody", "code", "no tenant"),
        ("markets-data", "kafka", "no service 'kafka'"),
        ("markets-data", "idle", "not deployed"),
        ("nocode", "code", "does not deploy a code server"),
    ],
)
def test_anything_the_tenant_file_does_not_declare_is_refused(tenant, service, message):
    with pytest.raises(SystemExit, match=message):
        restart.target(TENANTS, tenant, service)
