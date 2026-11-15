import pytest

from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import Config
from experimenttracker.slurm import DemoScheduler
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow


@pytest.fixture
def config(tmp_path):
    token = tmp_path / "token"
    token.write_text("t" * 40)
    return Config(
        state_dir=tmp_path / "state",
        job_output_root=tmp_path / "outputs",
        backup_dir=tmp_path / "backups",
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
