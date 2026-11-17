"""Tests for the incremental audit handoff API (/handoffs)."""

import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from joblog.main import create_app


@pytest.fixture()
def env(tmp_path: Path):
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    with client:
        yield {
            "app": app,
            "client": client,
            "root": tmp_path,
            "db": app.state.database,
            "manager": app.state.manager,
            "restores": app.state.restore_dir,
        }


def append(client: TestClient, job_id: str, message: str) -> dict:
    response = client.post("/logs", json={"job_id": job_id, "message": message})
    assert response.status_code == 201, response.text
    return response.json()


def make_backup(client: TestClient) -> dict:
    response = client.post("/backups")
    assert response.status_code == 201, response.text
    return response.json()


def establish(client: TestClient, consumer: str, backup_id: str) -> dict:
    response = client.post(
        "/handoffs", json={"consumer_id": consumer, "backup_id": backup_id}
    )
    assert response.status_code == 201, response.text
    return response.json()


def claim(client: TestClient, consumer: str, limit: int = 100) -> dict:
    response = client.post(f"/handoffs/{consumer}/claim", params={"limit": limit})
    assert response.status_code == 200, response.text
    return response.json()


def confirm(client: TestClient, consumer: str, token: str) -> dict:
    response = client.post(
        f"/handoffs/{consumer}/confirm", json={"batch_token": token}
    )
    assert response.status_code == 200, response.text
    return response.json()


def corrupt_backup(env: dict, backup_id: str) -> None:
    db_path = env["root"] / "backups" / backup_id / "joblogs.db"
    db_path.chmod(0o644)
    db_path.write_bytes(db_path.read_bytes()[:4096])  # truncate -> corrupt


def test_establish_anchors_at_verified_backup_end(env):
    client = env["client"]
    for i in range(3):
        append(client, "job-a", f"before-{i}")
    backup = make_backup(client)
    assert backup["snapshot_max_id"] == 3
    append(client, "job-a", "after-1")
    append(client, "job-a", "after-2")

    handoff = establish(client, "auditor-1", backup["id"])
    assert handoff["backup_id"] == backup["id"]
    assert handoff["start_after_id"] == 3
    assert handoff["next_id"] == 4
    assert handoff["open_batch"] is None

    batch = claim(client, "auditor-1")
    assert [item["message"] for item in batch["items"]] == ["after-1", "after-2"]
    assert batch["from_id"] == 4 and batch["to_id"] == 5


def test_establish_with_empty_backup_starts_at_zero(env):
    client = env["client"]
    backup = make_backup(client)  # empty database
    assert backup["snapshot_max_id"] is None

    handoff = establish(client, "auditor-empty", backup["id"])
    assert handoff["start_after_id"] == 0
    assert handoff["next_id"] == 1

    append(client, "job-a", "first-after-empty-backup")
    batch = claim(client, "auditor-empty")
    assert [item["id"] for item in batch["items"]] == [1]


def test_establish_retry_returns_original_anchor_and_conflict_on_other_backup(env):
    client = env["client"]
    append(client, "job-a", "v1")
    backup1 = make_backup(client)
    first = establish(client, "auditor-1", backup1["id"])

    # Same consumer + same backup: idempotent retry, original anchor, 200.
    retry = client.post(
        "/handoffs", json={"consumer_id": "auditor-1", "backup_id": backup1["id"]}
    )
    assert retry.status_code == 200, retry.text
    assert retry.json()["start_after_id"] == first["start_after_id"]
    assert retry.json()["created_at"] == first["created_at"]

    # Same consumer + different backup: conflict, existing progress untouched.
    append(client, "job-a", "v2")
    backup2 = make_backup(client)
    conflict = client.post(
        "/handoffs", json={"consumer_id": "auditor-1", "backup_id": backup2["id"]}
    )
    assert conflict.status_code == 409
    state = client.get("/handoffs/auditor-1").json()
    assert state["backup_id"] == backup1["id"]
    assert state["start_after_id"] == first["start_after_id"]

    # A different consumer may still use the second backup.
    other = establish(client, "auditor-2", backup2["id"])
    assert other["start_after_id"] == backup2["snapshot_max_id"]


def test_corrupt_or_unknown_backup_establishes_nothing(env):
    client = env["client"]
    append(client, "job-a", "good")
    backup = make_backup(client)
    corrupt_backup(env, backup["id"])

    corrupt = client.post(
        "/handoffs", json={"consumer_id": "auditor-x", "backup_id": backup["id"]}
    )
    assert corrupt.status_code == 409
    assert client.get("/handoffs/auditor-x").status_code == 404

    missing = client.post(
        "/handoffs",
        json={"consumer_id": "auditor-y", "backup_id": "bkp-20990101T000000Z-000000000000"},
    )
    assert missing.status_code == 404
    assert client.get("/handoffs/auditor-y").status_code == 404

    # The failure did not disturb other consumers or normal operation.
    healthy = make_backup(client)
    handoff = establish(client, "auditor-z", healthy["id"])
    assert handoff["start_after_id"] == healthy["snapshot_max_id"]


def test_claim_reclaim_confirm_and_appends_wait_for_next_batch(env):
    client = env["client"]
    for i in range(5):
        append(client, "job-a", f"seed-{i}")
    backup = make_backup(client)
    for i in range(4):
        append(client, "job-a", f"delta-{i}")  # ids 6..9
    establish(client, "auditor-1", backup["id"])

    first = claim(client, "auditor-1", limit=2)
    assert [item["id"] for item in first["items"]] == [6, 7]
    token1 = first["batch_token"]
    assert token1

    # Appends during the open batch are not part of it.
    append(client, "job-a", "during-open-1")
    append(client, "job-a", "during-open-2")

    # Re-claiming the unconfirmed batch returns identical content.
    again = claim(client, "auditor-1", limit=100)
    assert again["batch_token"] == token1
    assert again["items"] == first["items"]
    assert again["from_id"] == first["from_id"] and again["to_id"] == first["to_id"]

    state = confirm(client, "auditor-1", token1)
    assert state["next_id"] == 8
    assert state["open_batch"] is None

    # The interleaved appends arrive in the next batch, in order.
    second = claim(client, "auditor-1")
    assert [item["message"] for item in second["items"]] == [
        "delta-2",
        "delta-3",
        "during-open-1",
        "during-open-2",
    ]
    confirm(client, "auditor-1", second["batch_token"])

    # Nothing left: empty batch, no credential, no state change.
    empty = claim(client, "auditor-1")
    assert empty["items"] == [] and empty["batch_token"] is None
    assert client.get("/handoffs/auditor-1").json()["next_id"] == 12


def test_only_current_token_advances_progress(env):
    client = env["client"]
    backup = make_backup(client)
    for i in range(4):
        append(client, "job-a", f"m-{i}")
    establish(client, "auditor-1", backup["id"])

    batch1 = claim(client, "auditor-1", limit=2)
    token1 = batch1["batch_token"]
    confirm(client, "auditor-1", token1)
    batch2 = claim(client, "auditor-1", limit=2)
    token2 = batch2["batch_token"]

    # Late confirmation of the previous batch: rejected, nothing changes.
    late = client.post(
        f"/handoffs/auditor-1/confirm", json={"batch_token": token1}
    )
    assert late.status_code == 409
    # Foreign token: rejected, nothing changes.
    foreign = client.post(
        f"/handoffs/auditor-1/confirm", json={"batch_token": "not-the-token"}
    )
    assert foreign.status_code == 409
    state = client.get("/handoffs/auditor-1").json()
    assert state["next_id"] == 3
    assert state["open_batch"]["batch_token"] == token2

    # The current token works; repeating it afterwards is stale.
    confirm(client, "auditor-1", token2)
    assert client.get("/handoffs/auditor-1").json()["next_id"] == 5
    repeated = client.post(
        f"/handoffs/auditor-1/confirm", json={"batch_token": token2}
    )
    assert repeated.status_code == 409
    # Confirming with no open batch is also a conflict.
    idle = client.post(
        f"/handoffs/auditor-1/confirm", json={"batch_token": "whatever"}
    )
    assert idle.status_code == 409


def test_concurrent_claims_cannot_skip_logs(env):
    client = env["client"]
    backup = make_backup(client)
    for i in range(30):
        append(client, "job-a", f"c-{i}")
    establish(client, "auditor-1", backup["id"])

    results: list[dict] = []
    errors: list[BaseException] = []

    def worker():
        try:
            response = client.post(
                "/handoffs/auditor-1/claim", params={"limit": 10}
            )
            assert response.status_code == 200, response.text
            results.append(response.json())
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    # Every concurrent claim observed the same single open batch.
    tokens = {r["batch_token"] for r in results}
    assert len(tokens) == 1
    first_ids = {tuple(item["id"] for item in r["items"]) for r in results}
    assert first_ids == {tuple(range(1, 11))}

    # Exactly one confirmation can succeed; the rest are stale.
    token = tokens.pop()
    outcomes = []

    def confirmer():
        outcomes.append(
            client.post(
                "/handoffs/auditor-1/confirm", json={"batch_token": token}
            ).status_code
        )

    confirmers = [threading.Thread(target=confirmer) for _ in range(4)]
    for t in confirmers:
        t.start()
    for t in confirmers:
        t.join()
    assert sorted(outcomes) == [200, 409, 409, 409]

    # Drain the rest and verify no id was skipped or delivered twice.
    seen = list(range(1, 11))
    while True:
        batch = claim(client, "auditor-1", limit=7)
        if not batch["items"]:
            break
        seen.extend(item["id"] for item in batch["items"])
        confirm(client, "auditor-1", batch["batch_token"])
    assert seen == list(range(1, 31))


def test_handoff_state_survives_restart(env):
    client = env["client"]
    backup = make_backup(client)
    for i in range(5):
        append(client, "job-a", f"r-{i}")
    establish(client, "auditor-1", backup["id"])
    opened = claim(client, "auditor-1", limit=2)
    confirm(client, "auditor-1", opened["batch_token"])
    pending = claim(client, "auditor-1", limit=2)  # left unconfirmed
    root = env["root"]

    restarted = create_app(data_dir=root)
    with TestClient(restarted) as client2:
        state = client2.get("/handoffs/auditor-1").json()
        assert state["next_id"] == 3
        assert state["open_batch"]["batch_token"] == pending["batch_token"]

        # The persisted open batch is re-served identically after restart.
        reclaimed = claim(client2, "auditor-1")
        assert reclaimed["batch_token"] == pending["batch_token"]
        assert reclaimed["items"] == pending["items"]

        confirm(client2, "auditor-1", pending["batch_token"])
        final = claim(client2, "auditor-1")
        assert [item["id"] for item in final["items"]] == [5]
        confirm(client2, "auditor-1", final["batch_token"])
        assert client2.get("/handoffs/auditor-1").json()["next_id"] == 6


def test_failed_operations_do_not_change_progress(env):
    client = env["client"]
    append(client, "job-a", "seed")
    backup = make_backup(client)
    append(client, "job-a", "delta")
    establish(client, "auditor-1", backup["id"])
    opened = claim(client, "auditor-1")
    before = client.get("/handoffs/auditor-1").json()

    # Bad confirm.
    assert (
        client.post(
            "/handoffs/auditor-1/confirm", json={"batch_token": "bogus"}
        ).status_code
        == 409
    )
    # Conflicting establish.
    other_backup = make_backup(client)
    assert (
        client.post(
            "/handoffs",
            json={"consumer_id": "auditor-1", "backup_id": other_backup["id"]},
        ).status_code
        == 409
    )
    # Unknown consumer operations.
    assert client.get("/handoffs/ghost").status_code == 404
    assert client.post("/handoffs/ghost/claim").status_code == 404
    assert (
        client.post(
            "/handoffs/ghost/confirm", json={"batch_token": "x"}
        ).status_code
        == 404
    )

    after = client.get("/handoffs/auditor-1").json()
    assert after == before
    # The open batch is still the one originally issued and confirms fine.
    confirm(client, "auditor-1", opened["batch_token"])
    assert client.get("/handoffs/auditor-1").json()["next_id"] == 3


def test_handoff_coexists_with_append_backup_and_restore(env):
    client = env["client"]
    append(client, "job-a", "base")
    backup1 = make_backup(client)
    establish(client, "auditor-1", backup1["id"])
    append(client, "job-a", "delta-1")
    opened = claim(client, "auditor-1")  # batch stays open

    # Normal flows keep working while a batch is unconfirmed.
    append(client, "job-a", "delta-2")
    backup2 = make_backup(client)
    assert backup2["snapshot_max_id"] == 3
    restored = client.post(
        f"/backups/{backup2['id']}/restore", json={"target_name": "audit.db"}
    )
    assert restored.status_code == 200, restored.text
    assert restored.json()["row_count"] == 3

    # The open batch is unaffected; the rest arrives afterwards, in order.
    confirm(client, "auditor-1", opened["batch_token"])
    nxt = claim(client, "auditor-1")
    assert [item["message"] for item in nxt["items"]] == ["delta-2"]


def test_handoff_endpoints_in_openapi_and_docs(env):
    client = env["client"]
    assert client.get("/docs").status_code == 200
    schema = client.get("/openapi.json").json()
    for path in (
        "/handoffs",
        "/handoffs/{consumer_id}",
        "/handoffs/{consumer_id}/claim",
        "/handoffs/{consumer_id}/confirm",
    ):
        assert path in schema["paths"]
