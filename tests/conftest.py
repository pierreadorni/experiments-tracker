import contextlib
from pathlib import Path

import pytest

from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import Config
from experimenttracker.slurm import DemoScheduler
from experimenttracker.store import Store, encode
from experimenttracker.workflow import Workflow

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def config(tmp_path):
    token = tmp_path / "token"
    token.write_text("t" * 40)
    return Config(
        state_dir=tmp_path / "state",
        job_output_root=tmp_path / "outputs",
        backup_dir=tmp_path / "backups",
        recipe_dir=ROOT / "recipes/toy_compare",
        web_dir=tmp_path / "web",
        api_token_file=token,
        scheduler_seconds=0.1,
    )


@pytest.fixture
def runtime(config):
    store = Store(config.state_dir / "workflow.sqlite")
    scheduler = DemoScheduler(config.state_dir / "scheduler")
    agent = DemoAgent()
    workflow = Workflow(config, store, scheduler, agent)
    yield store, scheduler, agent, workflow
    store.close()


@pytest.fixture(autouse=True)
def historical_execution_fixtures(request, monkeypatch):
    """Existing suites exercise saved v1/v2 contracts, not the new default.

    New-contract behavior lives in test_iteration*. Creation still uses the real
    command boundary; only these historical fixture contracts are downgraded.
    """
    if request.node.path.name.startswith("test_iteration"):
        return
    original = Store.command

    def command(store, *args, **kwargs):
        result = original(store, *args, **kwargs)
        if len(args) > 1 and args[1] == "create" and result.get("experiment_id"):
            eid = result["experiment_id"]
            contract = store.experiment(eid)["contract"]
            if contract.pop("execution_contract", None) is not None:
                with (
                    contextlib.nullcontext(kwargs["_db"])
                    if kwargs.get("_db") is not None
                    else store.tx() as db
                ):
                    db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
                    store.emit(db, eid, "historical_fixture_loaded")
        return result

    monkeypatch.setattr(Store, "command", command)
