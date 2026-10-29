import pytest

from experimenttracker.cli import OwnershipLock
from experimenttracker.store import Store


def test_second_writer_fails_and_read_only_inspection_cannot_mutate(tmp_path):
    owner = OwnershipLock(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="another"):
            OwnershipLock(tmp_path)
        writer = Store(tmp_path / "workflow.sqlite")
        reader = Store.read_only(tmp_path / "workflow.sqlite")
        try:
            import sqlite3

            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                reader.db.execute("DELETE FROM experiments")
        finally:
            reader.close()
            writer.close()
    finally:
        owner.close()
    replacement = OwnershipLock(tmp_path)
    replacement.close()


def test_backup_cannot_replace_authoritative_state(runtime, config):
    store, _, _, _ = runtime
    with pytest.raises(ValueError, match="authoritative"):
        store.backup(config.state_dir)
    assert store.rows("PRAGMA integrity_check")[0]["integrity_check"] == "ok"


def test_backup_ignores_stale_temporary_symlinks_and_leaves_no_owned_temporary_file(runtime, tmp_path):
    store, _, _, _ = runtime
    destination = tmp_path / "backup"
    destination.mkdir()
    unrelated = tmp_path / "unrelated"
    unrelated.write_bytes(b"must not be replaced")
    (destination / "workflow.sqlite.pending").symlink_to(unrelated)
    store.backup(destination)
    assert unrelated.read_bytes() == b"must not be replaced"
    assert not list(destination.glob("workflow-*.pending"))
    backup = Store.read_only(destination / "workflow.sqlite")
    try:
        assert backup.rows("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
    finally:
        backup.close()
