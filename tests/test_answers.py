from uuid import uuid4

import pytest

from experimenttracker.models import request_answer
from experimenttracker.store import Conflict


def test_multiple_questions_require_complete_explicit_answers():
    pending = {"method": "item/tool/requestUserInput", "params": {"questions": [{"id": "a"}, {"id": "b"}]}}
    assert request_answer(pending, '{"a":"yes","b":["choice"]}') == {
        "answers": {"a": {"answers": ["yes"]}, "b": {"answers": ["choice"]}}
    }
    for invalid in ("yes", '{"a":"yes"}', '{"a":"yes","b":true}', '{"a":"yes","b":""}'):
        with pytest.raises(ValueError):
            request_answer(pending, invalid)


def test_invalid_approval_is_rejected_before_command_commit(runtime):
    store, _, _, _ = runtime
    eid = store.command(
        uuid4(), "create", {"brief": "Compare fixed synthetic models.", "recipe_id": "toy_compare_v1"}
    )["experiment_id"]
    pending = {"id": "pending", "method": "item/fileChange/requestApproval", "params": {}, "expired": False}
    store.update(eid, pending_request=pending)
    request_id = uuid4()
    with pytest.raises(Conflict, match="accept, decline, or cancel"):
        store.command(request_id, "answer", {"pending_request_id": "pending", "answer": "whatever"}, eid=eid)
    assert not store.one("SELECT * FROM commands WHERE request_id=?", (str(request_id),))
    assert "answer" not in store.experiment(eid)["pending_request"]
    assert request_answer(pending, "decline") == {"decision": "decline"}
