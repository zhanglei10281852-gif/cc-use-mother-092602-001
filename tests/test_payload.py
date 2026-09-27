from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.core.clock import FrozenClock, to_storage
from app.database import close_connection, get_connection, init_db
from app.payload.planner import simulate_spec
from app.payload.service import PayloadService

T0 = datetime(2026, 9, 26, 0, 0, 0, tzinfo=UTC)


def window_payload(key: str, **overrides) -> dict:
    now = datetime.now(UTC).replace(microsecond=0)
    payload = {
        "window_key": key,
        "name": f"散热窗口{key}",
        "starts_at": (now - timedelta(hours=1)).isoformat(timespec="seconds"),
        "ends_at": (now + timedelta(hours=1)).isoformat(timespec="seconds"),
        "max_power_watts": 100,
        "max_concurrent_tasks": 3,
        "safe_pause_margin_seconds": 0,
    }
    payload.update(overrides)
    return payload


def task_payload(request_id: str, *, user: str = "duty-engineer", seconds: int = 100, watts: float = 40, priority: int = 50) -> dict:
    return {
        "request_id": request_id,
        "requested_by": user,
        "estimated_seconds": seconds,
        "power_watts": watts,
        "priority": priority,
        "payload": {"experiment": request_id},
    }


def create_window(client, key: str = "w-api", **overrides) -> dict:
    response = client.post("/api/payload/windows", json=window_payload(key, **overrides))
    assert response.status_code == 201, response.text
    return response.json()


def submit(client, request_id: str, **kwargs) -> dict:
    response = client.post("/api/payload/tasks", json=task_payload(request_id, **kwargs))
    assert response.status_code == 202, response.text
    return response.json()


def test_window_configuration_idempotency_and_validation(client):
    created = create_window(client, "w-config")
    replay = client.post("/api/payload/windows", json=window_payload("w-config"))
    assert replay.status_code == 201
    assert replay.json()["id"] == created["id"]
    conflict = client.post("/api/payload/windows", json=window_payload("w-config", max_power_watts=200))
    assert conflict.status_code == 409
    invalid = window_payload("w-invalid")
    invalid["starts_at"], invalid["ends_at"] = invalid["ends_at"], invalid["starts_at"]
    assert client.post("/api/payload/windows", json=invalid).status_code == 422
    listing = client.get("/api/payload/windows").json()
    assert any(item["window_key"] == "w-config" and item["is_open"] for item in listing["items"])


def test_task_submission_idempotency_and_fingerprint_conflict(client):
    first = submit(client, "req-0001", seconds=120, priority=70)
    second = client.post("/api/payload/tasks", json=task_payload("req-0001", seconds=120, priority=70))
    assert second.status_code == 202
    assert second.json()["id"] == first["id"]
    conflict = client.post("/api/payload/tasks", json=task_payload("req-0001", seconds=121, priority=70))
    assert conflict.status_code == 409
    assert first["remaining_seconds"] == 120
    assert first["priority"] == 70
    assert first["status"] == "queued"


def test_claim_priority_order_and_power_budget(client):
    create_window(client, "w-claim", max_power_watts=100, max_concurrent_tasks=3)
    low = submit(client, "req-low", watts=60, priority=10)
    high = submit(client, "req-high", watts=60, priority=90)
    small = submit(client, "req-small", watts=30, priority=20)
    first = client.post("/api/payload/tasks/claim", json={"worker_id": "w1", "lease_seconds": 60}).json()
    assert first["reason"] == "claimed" and first["task"]["id"] == high["id"]
    second = client.post("/api/payload/tasks/claim", json={"worker_id": "w2", "lease_seconds": 60}).json()
    assert second["task"]["id"] == small["id"]
    third = client.post("/api/payload/tasks/claim", json={"worker_id": "w3", "lease_seconds": 60}).json()
    assert third["task"] is None and third["reason"] == "no_capacity"
    assert low["status"] == "queued"


def test_claim_without_open_window(client):
    future_start = (datetime.now(UTC) + timedelta(hours=2)).isoformat(timespec="seconds")
    future_end = (datetime.now(UTC) + timedelta(hours=3)).isoformat(timespec="seconds")
    create_window(client, "w-future", starts_at=future_start, ends_at=future_end)
    submit(client, "req-waiting")
    response = client.post("/api/payload/tasks/claim", json={"worker_id": "w1", "lease_seconds": 60}).json()
    assert response["task"] is None and response["reason"] == "no_open_window"


def test_heartbeat_pause_and_energy_accounting(client):
    window = create_window(client, "w-energy")
    task = submit(client, "req-pause", seconds=100, watts=40)
    claimed = client.post("/api/payload/tasks/claim", json={"worker_id": "w1", "lease_seconds": 60}).json()
    assert claimed["task"]["id"] == task["id"]
    heartbeat = client.post(f"/api/payload/tasks/{task['id']}/heartbeat", json={"worker_id": "w1", "progress_seconds": 30, "lease_seconds": 60})
    assert heartbeat.status_code == 200
    assert heartbeat.json()["task"]["progress_seconds"] == 30
    paused = client.post(f"/api/payload/tasks/{task['id']}/pause", json={"worker_id": "w1", "end_reason": "window_end", "progress_seconds": 45})
    assert paused.status_code == 200
    assert paused.json()["status"] == "paused"
    assert paused.json()["remaining_seconds"] == 55
    assert paused.json()["pause_count"] == 1
    detail = client.get(f"/api/payload/tasks/{task['id']}").json()
    assert len(detail["segments"]) == 1
    segment = detail["segments"][0]
    assert segment["executed_seconds"] == 45
    assert segment["energy_millijoules"] == 45 * 40_000
    assert segment["end_reason"] == "window_end"
    energy = client.get(f"/api/payload/windows/{window['id']}/energy").json()
    assert energy["consumed_millijoules"] == 45 * 40_000
    assert energy["tasks"] == [{"task_id": task["id"], "segments": 1, "executed_seconds": 45, "energy_millijoules": 45 * 40_000}]


def test_cross_window_resume_preserves_priority_and_remaining(client):
    create_window(client, "w-one")
    task = submit(client, "req-resume", seconds=100, watts=40, priority=88)
    client.post("/api/payload/tasks/claim", json={"worker_id": "w1", "lease_seconds": 60})
    client.post(f"/api/payload/tasks/{task['id']}/pause", json={"worker_id": "w1", "end_reason": "window_end", "progress_seconds": 45})
    windows = client.get("/api/payload/windows").json()["items"]
    first_window = next(item for item in windows if item["window_key"] == "w-one")
    closed = client.post(f"/api/payload/windows/{first_window['id']}/close", json={"actor": "duty-engineer", "reason": "窗口结束"})
    assert closed.status_code == 200
    create_window(client, "w-two")
    resumed = client.post("/api/payload/tasks/claim", json={"worker_id": "w2", "lease_seconds": 60}).json()
    assert resumed["reason"] == "claimed"
    assert resumed["task"]["id"] == task["id"]
    assert resumed["task"]["priority"] == 88
    assert resumed["task"]["remaining_seconds"] == 55
    assert resumed["task"]["pause_count"] == 1
    completed = client.post(
        f"/api/payload/tasks/{task['id']}/complete",
        json={"worker_id": "w2", "result": {"value": 3.14}, "metrics": {"checksum": "abc"}, "progress_seconds": 55},
    )
    assert completed.status_code == 200
    receipt = completed.json()["receipt"]
    assert receipt["total_executed_seconds"] == 100
    assert receipt["total_energy_millijoules"] == 100 * 40_000
    fetched = client.get(f"/api/payload/tasks/{task['id']}/receipt").json()
    assert fetched["receipt_digest"] == receipt["receipt_digest"]
    detail = client.get(f"/api/payload/tasks/{task['id']}").json()
    actions = [event["action"] for event in detail["audit_events"]]
    assert actions == ["task.submitted", "task.claimed", "task.paused", "task.resumed", "task.completed"]
    assert detail["status"] == "completed"


def test_window_close_pauses_running_tasks_with_records(client):
    window = create_window(client, "w-close")
    task = submit(client, "req-close", seconds=100, watts=40)
    client.post("/api/payload/tasks/claim", json={"worker_id": "w1", "lease_seconds": 60})
    client.post(f"/api/payload/tasks/{task['id']}/heartbeat", json={"worker_id": "w1", "progress_seconds": 10, "lease_seconds": 60})
    closed = client.post(f"/api/payload/windows/{window['id']}/close", json={"actor": "duty-engineer", "reason": "温度超限提前关闭"})
    assert closed.status_code == 200
    assert closed.json()["paused_task_ids"] == [task["id"]]
    detail = client.get(f"/api/payload/tasks/{task['id']}").json()
    assert detail["status"] == "paused"
    assert detail["remaining_seconds"] == 90
    assert detail["segments"][0]["end_reason"] == "window_end"
    again = client.post(f"/api/payload/windows/{window['id']}/close", json={"actor": "duty-engineer", "reason": "重复关闭"})
    assert again.status_code == 409


def test_cancel_resume_and_audit_query(client):
    create_window(client, "w-audit")
    cancelled = submit(client, "req-cancel", priority=10)
    running_cancel = submit(client, "req-running", priority=90)
    client.post("/api/payload/tasks/claim", json={"worker_id": "w1", "lease_seconds": 60})
    blocked = client.post(f"/api/payload/tasks/{running_cancel['id']}/cancel", json={"actor": "duty-engineer", "reason": "尝试取消运行任务"})
    assert blocked.status_code == 409
    done = client.post(f"/api/payload/tasks/{cancelled['id']}/cancel", json={"actor": "duty-engineer", "reason": "需求取消"})
    assert done.status_code == 200 and done.json()["status"] == "cancelled"
    paused = client.post(f"/api/payload/tasks/{running_cancel['id']}/pause", json={"worker_id": "w1", "end_reason": "operator_pause", "progress_seconds": 5})
    assert paused.status_code == 200
    resumed = client.post(f"/api/payload/tasks/{running_cancel['id']}/resume", json={"actor": "duty-engineer", "reason": "窗口恢复"})
    assert resumed.status_code == 200
    assert resumed.json()["status"] == "queued"
    assert resumed.json()["remaining_seconds"] == 95
    audit = client.get(f"/api/payload/audit?task_id={running_cancel['id']}").json()["items"]
    assert [event["action"] for event in audit] == ["task.submitted", "task.claimed", "task.paused", "task.requeued"]
    filtered = client.get(f"/api/payload/audit?action=task.cancelled").json()["items"]
    assert len(filtered) == 1 and filtered[0]["task_id"] == cancelled["id"]


def test_plan_endpoint_deterministic_and_matches_cli(client, capsys):
    spec = {
        "now": "2026-09-26T00:00:00+00:00",
        "windows": [
            {"window_key": "w1", "starts_at": "2026-09-26T00:00:00+00:00", "ends_at": "2026-09-26T00:01:00+00:00", "max_power_watts": 100, "max_concurrent_tasks": 2},
            {"window_key": "w2", "starts_at": "2026-09-26T01:00:00+00:00", "ends_at": "2026-09-26T01:01:00+00:00", "max_power_watts": 100, "max_concurrent_tasks": 2},
        ],
        "tasks": [
            {"task_key": "a", "priority": 90, "remaining_seconds": 90, "power_watts": 60},
            {"task_key": "b", "priority": 50, "remaining_seconds": 30, "power_watts": 40},
            {"task_key": "c", "priority": 10, "remaining_seconds": 200, "power_watts": 50},
        ],
    }
    first = client.post("/api/payload/plan", json=spec)
    second = client.post("/api/payload/plan", json=spec)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    plan = first.json()
    assert plan["totals"] == {"energy_millijoules": 8_100_000, "executed_seconds": 150, "completed_tasks": 2, "unfinished_tasks": 1}
    summaries = {item["task_key"]: item for item in plan["tasks"]}
    assert summaries["a"]["status"] == "completed" and summaries["a"]["energy_millijoules"] == 5_400_000
    assert summaries["b"]["status"] == "completed"
    assert summaries["c"]["status"] == "unfinished" and summaries["c"]["remaining_seconds"] == 170
    pauses = [segment for segment in plan["segments"] if segment["end_reason"] == "window_end"]
    assert len(pauses) == 2
    from app.cli import main as cli_main

    assert cli_main(["payload-plan", "--spec", json.dumps(spec)]) == 0
    cli_output = json.loads(capsys.readouterr().out)
    assert cli_output == plan


def test_planner_capacity_and_budget_rules():
    oversized_spec = {
        "windows": [{"window_key": "w1", "starts_at": "2026-09-26T00:00:00+00:00", "ends_at": "2026-09-26T00:10:00+00:00", "max_power_watts": 100}],
        "tasks": [{"task_key": "too-big", "remaining_seconds": 10, "power_watts": 200}],
    }
    oversized = simulate_spec(oversized_spec)
    assert oversized["tasks"][0]["unscheduled_reason"] == "power_exceeds_window_capacity"
    assert simulate_spec(oversized_spec) == oversized
    limited = simulate_spec(
        {
            "windows": [{"window_key": "w1", "starts_at": "2026-09-26T00:00:00+00:00", "ends_at": "2026-09-26T00:10:00+00:00", "max_power_watts": 100, "energy_budget_joules": 30}],
            "tasks": [{"task_key": "thirsty", "remaining_seconds": 100, "power_watts": 10}],
        }
    )
    segment = limited["segments"][0]
    assert segment["seconds"] == 3 and segment["end_reason"] == "budget_exhausted"
    assert limited["tasks"][0]["remaining_seconds"] == 97


@pytest.fixture()
def payload_service(tmp_path: Path):
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(tmp_path / "payload.db")
    close_connection()
    init_db()
    clock = FrozenClock(T0)
    service = PayloadService(get_connection(), clock)
    yield service, clock
    close_connection()


def service_window(key: str, start: datetime, end: datetime, **overrides) -> dict:
    payload = {
        "window_key": key,
        "name": key,
        "starts_at": start,
        "ends_at": end,
        "max_power_watts": 100,
        "max_concurrent_tasks": 2,
        "energy_budget_joules": 0,
        "safe_pause_margin_seconds": 0,
    }
    payload.update(overrides)
    return payload


def service_task(request_id: str, **overrides) -> dict:
    payload = {"request_id": request_id, "requested_by": "duty-engineer", "estimated_seconds": 500, "power_watts": 10, "priority": 60, "payload": {}}
    payload.update(overrides)
    return payload


def test_sweep_closes_expired_window_and_preserves_tasks(payload_service):
    service, clock = payload_service
    service.configure_window(service_window("w-expire", T0, T0 + timedelta(seconds=100)), "tester")
    task = service.submit_task(service_task("req-sweep"))
    claimed = service.claim("worker-a", 30)
    assert claimed["task"]["id"] == task["id"]
    clock.advance(seconds=150)
    result = service.sweep_expired()
    assert result["closed_window_ids"] and result["paused_task_ids"] == [task["id"]]
    detail = service.get_task(task["id"])
    assert detail["status"] == "paused"
    assert detail["remaining_seconds"] == 500
    assert detail["last_pause_reason"] == "window_end"
    assert detail["segments"][0]["end_reason"] == "window_end"
    later = T0 + timedelta(seconds=400)
    service.configure_window(service_window("w-next", T0 + timedelta(seconds=300), T0 + timedelta(seconds=900)), "tester")
    clock.current = later
    resumed = service.claim("worker-b", 30)
    assert resumed["task"]["id"] == task["id"]
    assert resumed["task"]["priority"] == 60
    assert resumed["task"]["remaining_seconds"] == 500


def test_sweep_pauses_expired_lease_without_closing_window(payload_service):
    service, clock = payload_service
    service.configure_window(service_window("w-long", T0, T0 + timedelta(seconds=10000)), "tester")
    task = service.submit_task(service_task("req-lease"))
    service.claim("worker-a", 30)
    clock.advance(seconds=31)
    result = service.sweep_expired()
    assert result["closed_window_ids"] == []
    assert result["paused_task_ids"] == [task["id"]]
    detail = service.get_task(task["id"])
    assert detail["last_pause_reason"] == "lease_expired"
    windows = service.list_windows()
    assert windows[0]["status"] == "scheduled"


def test_must_pause_by_and_heartbeat_warning(payload_service):
    service, clock = payload_service
    service.configure_window(service_window("w-margin", T0, T0 + timedelta(seconds=120), safe_pause_margin_seconds=10), "tester")
    task = service.submit_task(service_task("req-margin"))
    claimed = service.claim("worker-a", 300)
    assert claimed["must_pause_by"] == to_storage(T0 + timedelta(seconds=110))
    clock.advance(seconds=111)
    heartbeat = service.heartbeat(task["id"], "worker-a", 10, 60)
    assert heartbeat["should_pause"] is True
    assert heartbeat["window_remaining_seconds"] == 0


def test_energy_budget_limits_claiming(payload_service):
    service, clock = payload_service
    service.configure_window(service_window("w-budget", T0, T0 + timedelta(seconds=1000), energy_budget_joules=50), "tester")
    first = service.submit_task(service_task("req-budget-1", estimated_seconds=100))
    second = service.submit_task(service_task("req-budget-2", estimated_seconds=10))
    claimed = service.claim("worker-a", 600)
    assert claimed["task"]["id"] == first["id"]
    assert claimed["must_pause_by"] == to_storage(T0 + timedelta(seconds=5))
    heartbeat = service.heartbeat(first["id"], "worker-a", 5, 600)
    assert heartbeat["budget_remaining_millijoules"] == 0
    blocked = service.claim("worker-b", 60)
    assert blocked["task"] is None and blocked["reason"] == "no_capacity"
    service.pause(first["id"], "worker-a", "budget_exhausted", 5)
    still_blocked = service.claim("worker-b", 60)
    assert still_blocked["task"] is None
    detail = service.get_task(first["id"])
    assert detail["remaining_seconds"] == 95
    assert detail["segments"][0]["energy_millijoules"] == 50_000
    assert second["status"] == "queued"
