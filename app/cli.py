from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from app.database import database_path, get_connection, init_db
from app.main import app


def command_init() -> int:
    init_db()
    print(json.dumps({"database": str(database_path()), "status": "initialized"}, ensure_ascii=False))
    return 0


def command_check() -> int:
    init_db()
    connection = get_connection()
    result = {
        "database": str(database_path()),
        "integrity": connection.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_keys": connection.execute("PRAGMA foreign_keys").fetchone()[0],
        "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
        "tables": connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0],
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["integrity"] == "ok" and result["foreign_keys"] == 1 else 1


def command_smoke() -> int:
    with TestClient(app) as client:
        root = client.get("/")
        health = client.get("/api/system/health")
    result = {"root": root.json(), "health": health.json(), "status_codes": [root.status_code, health.status_code]}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status_codes"] == [200, 200] else 1


def command_compute_demo() -> int:
    template = {
        "code": "monte-carlo-demo",
        "name": "蒙特卡洛演示",
        "algorithm": "monte-carlo",
        "parameter_schema": {
            "samples": {"type": "integer", "required": True, "minimum": 10, "maximum": 1000000},
            "seed": {"type": "integer", "required": True},
        },
        "default_parameters": {},
        "max_runtime_seconds": 60,
        "max_attempts": 3,
    }
    with TestClient(app) as client:
        created = client.post("/api/compute/templates?actor=cli-demo", json=template)
        if created.status_code not in {201, 409}:
            print(created.text)
            return 1
        task = client.post(
            "/api/compute/tasks",
            json={
                "template_code": "monte-carlo-demo",
                "project_code": "demo",
                "requested_by": "cli-user",
                "parameters": {"samples": 1000, "seed": 42},
                "priority": 80,
                "idempotency_key": "compute-demo-000001",
            },
        )
        claimed = client.post(
            "/api/compute/tasks/claim",
            json={"worker_id": "cli-worker", "capabilities": ["monte-carlo"], "lease_seconds": 60},
        )
    result = {"task": task.status_code, "claimed": claimed.status_code, "task_id": task.json().get("id")}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if task.status_code == 202 and claimed.status_code == 200 and claimed.json().get("task") else 1


def command_payload_demo() -> int:
    """演示完整生命周期：开窗、提交、领取、安全暂停、跨窗口恢复、完成回执。"""
    now = datetime.now(UTC).replace(microsecond=0)
    first_start = now - timedelta(minutes=1)
    first_end = now + timedelta(minutes=30)
    second_start = first_end + timedelta(minutes=10)
    second_end = second_start + timedelta(minutes=30)
    with TestClient(app) as client:
        first_window = client.post(
            "/api/payload/windows?actor=duty-engineer",
            json={"name": "散热窗口-一", "starts_at": first_start.isoformat(), "ends_at": first_end.isoformat(),
                  "energy_budget_joules": 72000.0, "max_power_watts": 40.0},
        )
        if first_window.status_code not in {201, 409}:
            print(first_window.text)
            return 1
        second_window = client.post(
            "/api/payload/windows?actor=duty-engineer",
            json={"name": "散热窗口-二", "starts_at": second_start.isoformat(), "ends_at": second_end.isoformat(),
                  "energy_budget_joules": 72000.0, "max_power_watts": 40.0},
        )
        if second_window.status_code not in {201, 409}:
            print(second_window.text)
            return 1
        task = client.post(
            "/api/payload/tasks",
            json={"name": "成像数据压缩", "requested_by": "cli-user", "idempotency_key": "payload-demo-000001",
                  "estimated_seconds": 600, "power_watts": 20.0, "priority": 90},
        )
        replay = client.post(
            "/api/payload/tasks",
            json={"name": "成像数据压缩", "requested_by": "cli-user", "idempotency_key": "payload-demo-000001",
                  "estimated_seconds": 600, "power_watts": 20.0, "priority": 90},
        )
        claimed = client.post("/api/payload/tasks/claim", json={"worker_id": "payload-worker-1"})
        window_id = claimed.json().get("window", {}).get("id") if claimed.json().get("task") else None
        paused = client.post(f"/api/payload/windows/{window_id}/pause", json={"actor": "duty-engineer", "reason": "窗口即将结束，安全暂停"}) if window_id else None
        resumed = client.post("/api/payload/tasks/claim", json={"worker_id": "payload-worker-1"})
        task_id = task.json().get("id")
        completed = client.post(f"/api/payload/tasks/{task_id}/complete", json={"worker_id": "payload-worker-1", "result_summary": "压缩完成"}) if resumed.json().get("task") else None
        audit = client.get(f"/api/payload/audit?task_id={task_id}")
    result = {
        "task_id": task_id,
        "idempotent_replay": replay.json().get("idempotent_replay"),
        "claimed": bool(claimed.json().get("task")),
        "paused": len(paused.json().get("paused", [])) if paused else 0,
        "resumed": bool(resumed.json().get("task")),
        "receipt": completed.json().get("receipt") if completed else None,
        "audit_events": len(audit.json().get("items", [])),
    }
    print(json.dumps(result, ensure_ascii=False))
    ok = result["idempotent_replay"] and result["claimed"] and result["resumed"] and result["receipt"] and result["audit_events"] > 0
    return 0 if ok else 1


def command_payload_plan(at: str | None) -> int:
    """按指定时刻推演调度计划；同一数据库与同一 at 输出完全一致。"""
    from app.payload.service import PayloadService

    init_db()
    moment = datetime.fromisoformat(at) if at else None
    if moment is not None and moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    plan = PayloadService(get_connection()).plan(moment)
    print(json.dumps(plan, ensure_ascii=False, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="compute-operations", description="科学计算任务运营服务维护入口")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化 SQLite 数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行本地 API 冒烟检查")
    subparsers.add_parser("compute-demo", help="执行计算任务提交与领取演示")
    subparsers.add_parser("payload-demo", help="执行载荷热窗口暂停恢复演示")
    plan_parser = subparsers.add_parser("payload-plan", help="推演载荷调度计划（可用 --at 固定时刻复现）")
    plan_parser.add_argument("--at", default=None, help="推演时刻，ISO 8601 格式，缺省为当前时间")
    args = parser.parse_args()
    if args.command == "payload-plan":
        return command_payload_plan(args.at)
    handlers = {
        "init-db": command_init,
        "check-db": command_check,
        "smoke": command_smoke,
        "compute-demo": command_compute_demo,
        "payload-demo": command_payload_demo,
    }
    return handlers[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
