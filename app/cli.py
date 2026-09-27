from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from app.database import database_path, get_connection, init_db
from app.main import app
from app.payload.planner import simulate_spec


def command_init(_args: argparse.Namespace) -> int:
    init_db()
    print(json.dumps({"database": str(database_path()), "status": "initialized"}, ensure_ascii=False))
    return 0


def command_check(_args: argparse.Namespace) -> int:
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


def command_smoke(_args: argparse.Namespace) -> int:
    with TestClient(app) as client:
        root = client.get("/")
        health = client.get("/api/system/health")
    result = {"root": root.json(), "health": health.json(), "status_codes": [root.status_code, health.status_code]}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status_codes"] == [200, 200] else 1


def command_compute_demo(_args: argparse.Namespace) -> int:
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


def command_payload_plan(args: argparse.Namespace) -> int:
    """读取排期规格并输出确定性仿真结果，与 POST /api/payload/plan 完全一致。"""
    if args.input:
        spec = json.loads(Path(args.input).read_text(encoding="utf-8"))
    elif args.spec:
        spec = json.loads(args.spec)
    else:
        print(json.dumps({"error": "必须通过 --spec 或 --input 提供排期规格"}, ensure_ascii=False))
        return 1
    plan = simulate_spec(spec)
    print(json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


def command_payload_demo(_args: argparse.Namespace) -> int:
    """端到端演示：配置窗口、幂等提交、领取、心跳、暂停、跨窗口恢复与完成回执。"""
    now = datetime.now(UTC).replace(microsecond=0)
    suffix = str(int(datetime.now(UTC).timestamp() * 1000))

    def fmt(value: datetime) -> str:
        return value.isoformat(timespec="seconds")

    with TestClient(app) as client:
        window_one = client.post(
            "/api/payload/windows",
            json={
                "window_key": f"cli-demo-window-1-{suffix}",
                "name": "演示窗口一",
                "starts_at": fmt(now - timedelta(minutes=10)),
                "ends_at": fmt(now + timedelta(minutes=30)),
                "max_power_watts": 120,
                "max_concurrent_tasks": 2,
                "safe_pause_margin_seconds": 5,
            },
        )
        if window_one.status_code not in {200, 201}:
            print(window_one.text)
            return 1
        window_id = window_one.json()["id"]
        submitted = client.post(
            "/api/payload/tasks",
            json={
                "request_id": f"cli-demo-request-{suffix}",
                "requested_by": "cli-engineer",
                "estimated_seconds": 120,
                "power_watts": 40,
                "priority": 80,
                "payload": {"experiment": "thermal-demo"},
            },
        )
        replayed = client.post(
            "/api/payload/tasks",
            json={
                "request_id": f"cli-demo-request-{suffix}",
                "requested_by": "cli-engineer",
                "estimated_seconds": 120,
                "power_watts": 40,
                "priority": 80,
                "payload": {"experiment": "thermal-demo"},
            },
        )
        task_id = submitted.json()["id"]
        claimed = client.post("/api/payload/tasks/claim", json={"worker_id": "cli-worker", "lease_seconds": 120})
        heartbeat = client.post(f"/api/payload/tasks/{task_id}/heartbeat", json={"worker_id": "cli-worker", "progress_seconds": 30, "lease_seconds": 120})
        paused = client.post(f"/api/payload/tasks/{task_id}/pause", json={"worker_id": "cli-worker", "end_reason": "window_end", "progress_seconds": 45})
        closed = client.post(f"/api/payload/windows/{window_id}/close", json={"actor": "cli-engineer", "reason": "演示窗口结束"})
        window_two = client.post(
            "/api/payload/windows",
            json={
                "window_key": f"cli-demo-window-2-{suffix}",
                "name": "演示窗口二",
                "starts_at": fmt(now - timedelta(minutes=1)),
                "ends_at": fmt(now + timedelta(minutes=60)),
                "max_power_watts": 120,
                "max_concurrent_tasks": 2,
                "safe_pause_margin_seconds": 5,
            },
        )
        resumed = client.post("/api/payload/tasks/claim", json={"worker_id": "cli-worker", "lease_seconds": 120})
        completed = client.post(
            f"/api/payload/tasks/{task_id}/complete",
            json={"worker_id": "cli-worker", "result": {"value": 42}, "metrics": {"checksum": "demo"}, "progress_seconds": 75},
        )
        receipt = client.get(f"/api/payload/tasks/{task_id}/receipt")
        audit = client.get(f"/api/payload/audit?task_id={task_id}")
    resumed_task = resumed.json().get("task") or {}
    checks = {
        "window_created": window_one.status_code == 201,
        "idempotent_replay": submitted.status_code == replayed.status_code == 202 and replayed.json()["id"] == task_id,
        "claimed": claimed.status_code == 200 and claimed.json()["task"]["id"] == task_id,
        "heartbeat": heartbeat.status_code == 200,
        "paused_with_remaining": paused.status_code == 200 and paused.json()["remaining_seconds"] == 75,
        "window_closed": closed.status_code == 200,
        "second_window": window_two.status_code == 201,
        "resumed_across_windows": resumed.status_code == 200 and resumed_task.get("id") == task_id and resumed_task.get("remaining_seconds") == 75 and resumed_task.get("priority") == 80,
        "completed_with_receipt": completed.status_code == 200 and receipt.status_code == 200 and receipt.json()["total_executed_seconds"] == 120,
        "audit_trail": audit.status_code == 200 and len(audit.json()["items"]) >= 5,
    }
    result = {"checks": checks, "task_id": task_id, "receipt_digest": receipt.json().get("receipt_digest") if receipt.status_code == 200 else None}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(checks.values()) else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="compute-operations", description="科学计算任务运营服务维护入口")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化 SQLite 数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行本地 API 冒烟检查")
    subparsers.add_parser("compute-demo", help="执行计算任务提交与领取演示")
    plan_parser = subparsers.add_parser("payload-plan", help="对热窗口排期规格执行确定性仿真")
    plan_parser.add_argument("--spec", help="排期规格 JSON 字符串")
    plan_parser.add_argument("--input", help="排期规格 JSON 文件路径")
    subparsers.add_parser("payload-demo", help="执行热窗口提交、暂停、恢复与回执演示")
    args = parser.parse_args(argv)
    handlers = {
        "init-db": command_init,
        "check-db": command_check,
        "smoke": command_smoke,
        "compute-demo": command_compute_demo,
        "payload-plan": command_payload_plan,
        "payload-demo": command_payload_demo,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
