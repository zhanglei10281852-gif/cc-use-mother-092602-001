from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection, init_db
from app.payload.service import PayloadService, joules

T0 = datetime(2026, 9, 26, 2, 0, tzinfo=UTC)

WINDOW_ONE = {"name": "窗口-一", "starts_at": "2026-09-26T01:00:00+00:00", "ends_at": "2026-09-26T03:00:00+00:00",
              "energy_budget_joules": 36000.0, "max_power_watts": 40.0}
WINDOW_TWO = {"name": "窗口-二", "starts_at": "2026-09-26T04:00:00+00:00", "ends_at": "2026-09-26T05:00:00+00:00",
              "energy_budget_joules": 36000.0, "max_power_watts": 40.0}
FUTURE_WINDOW = {"name": "窗口-未来", "starts_at": "2099-01-01T01:00:00+00:00", "ends_at": "2099-01-01T03:00:00+00:00",
                 "energy_budget_joules": 36000.0, "max_power_watts": 40.0}


def task_payload(key: str, *, name: str = "成像压缩", priority: int = 50, seconds: int = 100, power: float = 20.0, user: str = "duty-1") -> dict:
    return {"name": name, "requested_by": user, "idempotency_key": key,
            "estimated_seconds": seconds, "power_watts": power, "priority": priority}


def make_service(clock: FrozenClock) -> PayloadService:
    init_db()
    return PayloadService(get_connection(), clock)


def test_window_validation_and_overlap(client):
    created = client.post("/api/payload/windows?actor=duty-1", json=FUTURE_WINDOW)
    assert created.status_code == 201, created.text
    assert created.json()["status"] == "scheduled"
    assert created.json()["energy_remaining_joules"] == 36000.0
    invalid = dict(FUTURE_WINDOW, name="窗口-非法", ends_at=FUTURE_WINDOW["starts_at"])
    assert client.post("/api/payload/windows?actor=duty-1", json=invalid).status_code == 422
    duplicate = client.post("/api/payload/windows?actor=duty-1", json=FUTURE_WINDOW)
    assert duplicate.status_code == 409
    overlap = dict(FUTURE_WINDOW, name="窗口-重叠", starts_at="2099-01-01T02:00:00+00:00")
    assert client.post("/api/payload/windows?actor=duty-1", json=overlap).status_code == 409
    listing = client.get("/api/payload/windows")
    assert listing.status_code == 200 and len(listing.json()["items"]) == 1


def test_submit_idempotency(client):
    first = client.post("/api/payload/tasks", json=task_payload("req-000001"))
    second = client.post("/api/payload/tasks", json=task_payload("req-000001"))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["idempotent_replay"] is False
    assert second.json()["idempotent_replay"] is True
    changed = task_payload("req-000001", priority=99)
    conflict = client.post("/api/payload/tasks", json=changed)
    assert conflict.status_code == 409
    other_user = client.post("/api/payload/tasks", json=task_payload("req-000001", user="duty-2"))
    assert other_user.status_code == 202 and other_user.json()["id"] != first.json()["id"]


def test_claim_gating_and_priority_order(client):
    clock = FrozenClock(T0)
    service = make_service(clock)
    assert service.claim("worker-1")["reason"] == "no_active_window"
    service.create_window(WINDOW_ONE, "duty-1")
    service.submit(task_payload("power-too-high", power=50.0))
    assert service.claim("worker-1")["reason"] == "no_eligible_task"
    low = service.submit(task_payload("prio-low", priority=10))
    high = service.submit(task_payload("prio-high", priority=90))
    claimed = service.claim("worker-1")
    assert claimed["task"]["id"] == high["id"]
    assert claimed["window"]["id"] is not None
    second = service.claim("worker-2")
    assert second["task"]["id"] == low["id"]


def test_claim_respects_window_energy_budget(client):
    clock = FrozenClock(T0)
    service = make_service(clock)
    service.create_window(dict(WINDOW_ONE, energy_budget_joules=36.0), "duty-1")
    first = service.submit(task_payload("energy-first", power=12.0, seconds=100))
    assert service.claim("worker-1")["task"]["id"] == first["id"]
    clock.advance(seconds=3)
    service.pause_window_tasks(1, "duty-1", "窗口即将结束")
    assert service.get_window(1)["energy_remaining_joules"] == 0.0
    service.submit(task_payload("energy-second", power=1.0, seconds=10))
    assert service.claim("worker-2")["reason"] == "window_energy_exhausted"


def test_pause_preserves_priority_remaining_and_writes_audit(client):
    clock = FrozenClock(T0)
    service = make_service(clock)
    service.create_window(WINDOW_ONE, "duty-1")
    task = service.submit(task_payload("pause-me", priority=77, seconds=100, power=25.0))
    service.claim("worker-1")
    clock.advance(seconds=30)
    result = service.pause_window_tasks(1, "duty-1", "窗口即将结束")
    paused = result["paused"][0]
    assert paused["status"] == "paused"
    assert paused["priority"] == 77
    assert paused["remaining_seconds"] == 70
    assert paused["total_run_seconds"] == 30
    assert paused["energy_joules"] == joules(25.0, 30) == 750.0
    assert paused["pause_count"] == 1
    assert paused["worker_id"] == ""
    events = service.audit(task_id=task["id"])
    assert [event["event_type"] for event in events] == ["task_submitted", "task_claimed", "task_paused"]
    pause_event = json.loads(events[-1]["details_json"])
    assert pause_event["elapsed_seconds"] == 30 and pause_event["remaining_seconds"] == 70
    window_events = service.audit(window_id=1, event_type="window_paused")
    assert len(window_events) == 1


def test_pause_after_window_end_clamps_to_window_close(client):
    clock = FrozenClock(datetime(2026, 9, 26, 1, 59, 50, tzinfo=UTC))
    service = make_service(clock)
    service.create_window(dict(WINDOW_ONE, ends_at="2026-09-26T02:00:00+00:00"), "duty-1")
    task = service.submit(task_payload("clamp-me", seconds=1000, power=10.0))
    service.claim("worker-1")
    clock.advance(seconds=30)
    paused = service.pause_window_tasks(1, "duty-1", "窗口已经结束")["paused"][0]
    assert paused["total_run_seconds"] == 10
    assert paused["remaining_seconds"] == 990
    assert paused["energy_joules"] == joules(10.0, 10)


def test_cross_window_resume_and_completion_receipt(client):
    clock = FrozenClock(datetime(2026, 9, 26, 1, 30, tzinfo=UTC))
    service = make_service(clock)
    service.create_window(WINDOW_ONE, "duty-1")
    service.create_window(WINDOW_TWO, "duty-1")
    task = service.submit(task_payload("resume-me", priority=88, seconds=100, power=10.0))
    service.claim("worker-1")
    clock.advance(seconds=20)
    service.pause_window_tasks(1, "duty-1", "窗口即将结束")
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-1", "暂停后不能完成")
    clock.current = datetime(2026, 9, 26, 4, 10, tzinfo=UTC)
    resumed = service.claim("worker-2")
    assert resumed["task"]["id"] == task["id"]
    assert resumed["task"]["priority"] == 88
    assert resumed["task"]["remaining_seconds"] == 80
    assert resumed["window"]["name"] == "窗口-二"
    clock.advance(seconds=40)
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-1", "不是持有者")
    done = service.complete(task["id"], "worker-2", "压缩完成")
    receipt = done["receipt"]
    assert done["idempotent_replay"] is False
    assert done["task"]["status"] == "completed"
    assert done["task"]["remaining_seconds"] == 0
    assert receipt["total_run_seconds"] == 60
    assert receipt["total_energy_joules"] == joules(10.0, 60) == 600.0
    assert receipt["windows_used"] == 2
    replay = service.complete(task["id"], "worker-2", "重复回执")
    assert replay["idempotent_replay"] is True
    assert replay["receipt"]["id"] == receipt["id"]
    events = [event["event_type"] for event in service.audit(task_id=task["id"])]
    assert events == ["task_submitted", "task_claimed", "task_paused", "task_resumed", "task_completed"]
    detail = service.get_task(task["id"])
    assert detail["receipt"]["id"] == receipt["id"]


def test_plan_is_deterministic_and_matches_energy_math(client):
    clock = FrozenClock(datetime(2026, 9, 26, 0, 59, tzinfo=UTC))
    service = make_service(clock)
    service.create_window(WINDOW_ONE, "duty-1")
    service.create_window(dict(WINDOW_TWO, energy_budget_joules=37000.0), "duty-1")
    big = service.submit(task_payload("plan-big", priority=90, seconds=3600, power=20.0))
    service.submit(task_payload("plan-overpower", priority=80, seconds=10, power=50.0))
    small = service.submit(task_payload("plan-small", priority=10, seconds=100, power=10.0))
    at = datetime(2026, 9, 26, 0, 59, tzinfo=UTC)
    first = service.plan(at)
    second = service.plan(at)
    assert first == second
    assert first["generated_at"] == "2026-09-26T00:59:00+00:00"
    big_plan = next(item for item in first["tasks"] if item["task_id"] == big["id"])
    assert big_plan["segments"] == [
        {"window_id": 1, "start": "2026-09-26T01:00:00+00:00", "end": "2026-09-26T01:30:00+00:00", "seconds": 1800, "energy_joules": 36000.0},
        {"window_id": 2, "start": "2026-09-26T04:00:00+00:00", "end": "2026-09-26T04:30:00+00:00", "seconds": 1800, "energy_joules": 36000.0},
    ]
    assert big_plan["completes_at"] == "2026-09-26T04:30:00+00:00"
    small_plan = next(item for item in first["tasks"] if item["task_id"] == small["id"])
    assert small_plan["segments"][0]["energy_joules"] == 1000.0
    assert small_plan["completes_at"] == "2026-09-26T04:31:40+00:00"
    assert first["unscheduled"] == [{"task_id": 2, "name": "成像压缩", "reason": "power_exceeds_window_limits"}]
    assert first["windows"][0]["planned_energy_joules"] == 36000.0
    assert first["windows"][1]["planned_energy_joules"] == 37000.0
    assert first["windows"][1]["energy_remaining_joules"] == 0.0
    api_plan = client.get("/api/payload/plan", params={"at": "2026-09-26T00:59:00+00:00"})
    assert api_plan.status_code == 200
    assert api_plan.json() == json.loads(json.dumps(first))
    env = dict(os.environ)
    cli = subprocess.run(
        [sys.executable, "-m", "app.cli", "payload-plan", "--at", "2026-09-26T00:59:00+00:00"],
        capture_output=True, text=True, env=env, cwd="/workspace",
    )
    assert cli.returncode == 0, cli.stderr
    assert json.loads(cli.stdout) == api_plan.json()


def test_plan_settles_running_task_in_flight(client):
    clock = FrozenClock(T0)
    service = make_service(clock)
    service.create_window(dict(WINDOW_ONE, energy_budget_joules=10000.0), "duty-1")
    task = service.submit(task_payload("in-flight", seconds=1000, power=10.0))
    service.claim("worker-1")
    clock.advance(seconds=30)
    plan = service.plan(clock.now())
    entry = next(item for item in plan["tasks"] if item["task_id"] == task["id"])
    assert entry["remaining_seconds"] == 970
    assert entry["segments"] == [
        {"window_id": 1, "start": "2026-09-26T02:00:30+00:00", "end": "2026-09-26T02:16:40+00:00", "seconds": 970, "energy_joules": 9700.0}
    ]
    assert entry["completes_at"] == "2026-09-26T02:16:40+00:00"
    assert plan["windows"][0]["planned_energy_joules"] == 10000.0
    assert plan["windows"][0]["energy_remaining_joules"] == 0.0


def test_audit_filters_via_api(client):
    client.post("/api/payload/windows?actor=duty-1", json=FUTURE_WINDOW)
    task = client.post("/api/payload/tasks", json=task_payload("audit-me")).json()
    by_type = client.get("/api/payload/audit", params={"event_type": "task_submitted"})
    assert by_type.status_code == 200
    assert [item["task_id"] for item in by_type.json()["items"]] == [task["id"]]
    by_window = client.get("/api/payload/audit", params={"window_id": 1})
    assert [item["event_type"] for item in by_window.json()["items"]] == ["window_created"]
    detail = client.get(f"/api/payload/tasks/{task['id']}")
    assert detail.status_code == 200
    assert detail.json()["receipt"] is None
    assert len(detail.json()["events"]) == 1
    missing = client.get("/api/payload/tasks/9999")
    assert missing.status_code == 404
