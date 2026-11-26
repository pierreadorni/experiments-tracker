import httpx
from helpers import seed_saved_contract4

from experimenttracker.config import GatewayConfig
from experimenttracker.gateway_client import GatewayClient


def publication_client(config, store, tmp_path, handler):
    token = tmp_path / "publication-token"
    token.write_text("p" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=token)
    return GatewayClient(config, store, transport=httpx.MockTransport(handler))


def publication_experiment(store):
    eid = seed_saved_contract4(store, "Compare the fixed synthetic models.")
    assert store.experiment(eid)["contract"]["execution_contract"] == 4
    with store.tx() as db:
        store.emit(db, eid, "experiment_created")
    return eid


def test_reference_outbox_migration_preserves_saved_history(runtime):
    from experimenttracker.store import Store

    store, *_ = runtime
    eid = publication_experiment(store)
    with store.tx() as db:
        db.execute("ALTER TABLE jobs DROP COLUMN observation_error")
        db.execute("DROP TABLE experiment_branches")
        db.execute("DROP TABLE obligations")
        db.execute("DROP TABLE artifact_registrations")
        db.execute("DELETE FROM schema_version WHERE version>=23")
    before = {name: store.rows(f"SELECT * FROM {name}") for name in ("experiments", "events", "outbox")}
    migrated = Store(store.path)
    try:
        assert migrated.one("SELECT MAX(version) AS version FROM schema_version")["version"] == 26
        assert migrated.rows("SELECT * FROM artifact_registrations") == []
        assert migrated.rows("SELECT * FROM obligations") == []
        assert migrated.rows("SELECT * FROM experiment_branches") == []
        assert {name: migrated.rows(f"SELECT * FROM {name}") for name in before} == before
        reader = Store.read_only(store.path)
        try:
            assert reader.projection(eid) == migrated.projection(eid)
        finally:
            reader.close()
    finally:
        migrated.close()
