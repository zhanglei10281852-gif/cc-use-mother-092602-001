"""卫星计算载荷热窗口调度服务。

职责：热窗口配置、计算请求提交（幂等）、排队领取、心跳进度、
安全暂停与跨窗口恢复、完成回执、能量核算和审计查询。
所有状态变更都在即时事务内完成，并写入不可变审计事件。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import request_fingerprint
from app.database import get_connection, transaction
from app.payload.planner import joules_to_millijoules, simulate_spec, watts_to_milliwatts

SCHEMA = """
CREATE TABLE IF NOT EXISTS payload_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    window_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    max_power_milliwatts INTEGER NOT NULL CHECK(max_power_milliwatts > 0),
    max_concurrent_tasks INTEGER NOT NULL DEFAULT 1 CHECK(max_concurrent_tasks > 0),
    energy_budget_millijoules INTEGER NOT NULL DEFAULT 0 CHECK(energy_budget_millijoules >= 0),
    safe_pause_margin_seconds INTEGER NOT NULL DEFAULT 0 CHECK(safe_pause_margin_seconds >= 0),
    request_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled' CHECK(status IN ('scheduled','closed')),
    closed_at TEXT,
    close_reason TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK(ends_at > starts_at)
);
CREATE TABLE IF NOT EXISTS payload_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    priority INTEGER NOT NULL DEFAULT 50 CHECK(priority BETWEEN 0 AND 100),
    estimated_seconds INTEGER NOT NULL CHECK(estimated_seconds > 0),
    remaining_seconds INTEGER NOT NULL CHECK(remaining_seconds >= 0),
    progress_seconds INTEGER NOT NULL DEFAULT 0 CHECK(progress_seconds >= 0),
    power_milliwatts INTEGER NOT NULL CHECK(power_milliwatts > 0),
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','paused','completed','cancelled')),
    window_id INTEGER REFERENCES payload_windows(id),
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT NOT NULL DEFAULT '',
    segment_started_at TEXT,
    pause_count INTEGER NOT NULL DEFAULT 0,
    last_pause_reason TEXT NOT NULL DEFAULT '',
    result_receipt_id INTEGER,
    version INTEGER NOT NULL DEFAULT 1,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(requested_by, request_id)
);
CREATE INDEX IF NOT EXISTS idx_payload_tasks_queue ON payload_tasks(status,priority DESC,created_at);
CREATE INDEX IF NOT EXISTS idx_payload_tasks_window ON payload_tasks(window_id,status);
CREATE TABLE IF NOT EXISTS payload_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES payload_tasks(id) ON DELETE CASCADE,
    window_id INTEGER REFERENCES payload_windows(id),
    worker_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    reported_seconds INTEGER NOT NULL CHECK(reported_seconds >= 0),
    executed_seconds INTEGER NOT NULL CHECK(executed_seconds >= 0),
    energy_millijoules INTEGER NOT NULL CHECK(energy_millijoules >= 0),
    end_reason TEXT NOT NULL CHECK(end_reason IN ('completed','window_end','operator_pause','lease_expired','budget_exhausted')),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payload_segments_task ON payload_segments(task_id,id);
CREATE INDEX IF NOT EXISTS idx_payload_segments_window ON payload_segments(window_id,id);
CREATE TABLE IF NOT EXISTS payload_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL UNIQUE REFERENCES payload_tasks(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL,
    result_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    total_executed_seconds INTEGER NOT NULL,
    total_energy_millijoules INTEGER NOT NULL,
    receipt_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS payload_audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    window_id INTEGER,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payload_audit_task ON payload_audit_events(task_id,id);
CREATE INDEX IF NOT EXISTS idx_payload_audit_window ON payload_audit_events(window_id,id);
CREATE INDEX IF NOT EXISTS idx_payload_audit_created ON payload_audit_events(created_at,id);
"""


def ensure_schema() -> None:
    get_connection().executescript(SCHEMA)


class PayloadService:
    """热窗口内的计算载荷任务调度事务服务。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        ensure_schema()

    # ------------------------------------------------------------------
    # 热窗口配置
    # ------------------------------------------------------------------
    def configure_window(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        starts_at = to_storage(payload["starts_at"])
        ends_at = to_storage(payload["ends_at"])
        max_power = watts_to_milliwatts(payload["max_power_watts"])
        if max_power < 1:
            raise ValidationError("窗口功率上限换算后不足 1 毫瓦")
        budget = joules_to_millijoules(payload.get("energy_budget_joules", 0))
        fingerprint = request_fingerprint(
            {
                "window_key": payload["window_key"],
                "name": payload["name"],
                "starts_at": starts_at,
                "ends_at": ends_at,
                "max_power_milliwatts": max_power,
                "max_concurrent_tasks": payload["max_concurrent_tasks"],
                "energy_budget_millijoules": budget,
                "safe_pause_margin_seconds": payload["safe_pause_margin_seconds"],
            }
        )
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            existing = connection.execute("SELECT * FROM payload_windows WHERE window_key=?", (payload["window_key"],)).fetchone()
            if existing is not None:
                if existing["request_fingerprint"] != fingerprint:
                    raise ConflictError("同一窗口标识对应了不同的窗口参数")
                return self._window_view(connection, existing, now)
            cursor = connection.execute(
                "INSERT INTO payload_windows(window_key,name,starts_at,ends_at,max_power_milliwatts,max_concurrent_tasks,energy_budget_millijoules,safe_pause_margin_seconds,request_fingerprint,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    payload["window_key"],
                    payload["name"],
                    starts_at,
                    ends_at,
                    max_power,
                    payload["max_concurrent_tasks"],
                    budget,
                    payload["safe_pause_margin_seconds"],
                    fingerprint,
                    actor,
                    now,
                    now,
                ),
            )
            self._audit(connection, window_id=cursor.lastrowid, task_id=None, action="window.configured", actor=actor, details={"window_key": payload["window_key"], "starts_at": starts_at, "ends_at": ends_at}, now=now)
            row = connection.execute("SELECT * FROM payload_windows WHERE id=?", (cursor.lastrowid,)).fetchone()
            return self._window_view(connection, row, now)

    def list_windows(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        now = to_storage(self.clock.now())
        if status:
            rows = self.connection.execute("SELECT * FROM payload_windows WHERE status=? ORDER BY starts_at, id LIMIT ?", (status, limit)).fetchall()
        else:
            rows = self.connection.execute("SELECT * FROM payload_windows ORDER BY starts_at, id LIMIT ?", (limit,)).fetchall()
        return [self._window_view(self.connection, row, now) for row in rows]

    def get_window(self, window_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM payload_windows WHERE id=?", (window_id,)).fetchone()
        if row is None:
            raise NotFoundError("热窗口不存在")
        now = to_storage(self.clock.now())
        view = self._window_view(self.connection, row, now)
        segments = self.connection.execute("SELECT * FROM payload_segments WHERE window_id=? ORDER BY id", (window_id,)).fetchall()
        view["segments"] = [self._segment_view(segment) for segment in segments]
        return view

    def close_window(self, window_id: int, actor: str, reason: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM payload_windows WHERE id=?", (window_id,)).fetchone()
            if row is None:
                raise NotFoundError("热窗口不存在")
            if row["status"] == "closed":
                raise ConflictError("热窗口已经关闭")
            paused = self._pause_running_tasks(connection, window_id, now_value, "window_end", actor)
            connection.execute("UPDATE payload_windows SET status='closed',closed_at=?,close_reason=?,updated_at=? WHERE id=?", (now, reason, now, window_id))
            self._audit(connection, window_id=window_id, task_id=None, action="window.closed", actor=actor, details={"reason": reason, "paused_task_ids": paused}, now=now)
            updated = connection.execute("SELECT * FROM payload_windows WHERE id=?", (window_id,)).fetchone()
            return {"window": self._window_view(connection, updated, now), "paused_task_ids": paused}

    def sweep_expired(self, actor: str = "payload-sweeper") -> dict[str, Any]:
        """关闭已过期窗口并安全暂停其任务，同时处理租约过期的运行任务。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        closed_windows: list[int] = []
        paused_tasks: list[int] = []
        with transaction(immediate=True) as connection:
            expired = connection.execute("SELECT * FROM payload_windows WHERE status='scheduled' AND ends_at<=? ORDER BY id", (now,)).fetchall()
            for window in expired:
                paused = self._pause_running_tasks(connection, window["id"], now_value, "window_end", actor)
                connection.execute("UPDATE payload_windows SET status='closed',closed_at=?,close_reason='窗口过期自动关闭',updated_at=? WHERE id=?", (now, now, window["id"]))
                self._audit(connection, window_id=window["id"], task_id=None, action="window.closed", actor=actor, details={"reason": "窗口过期自动关闭", "paused_task_ids": paused}, now=now)
                closed_windows.append(int(window["id"]))
                paused_tasks.extend(paused)
            stale = connection.execute("SELECT * FROM payload_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in stale:
                self._pause_task(connection, task, now_value, "lease_expired", actor, progress=None)
                paused_tasks.append(int(task["id"]))
        return {"closed_window_ids": closed_windows, "paused_task_ids": paused_tasks}

    # ------------------------------------------------------------------
    # 任务提交（幂等）
    # ------------------------------------------------------------------
    def submit_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        power = watts_to_milliwatts(payload["power_watts"])
        if power < 1:
            raise ValidationError("任务功率换算后不足 1 毫瓦")
        fingerprint = request_fingerprint(
            {
                "request_id": payload["request_id"],
                "requested_by": payload["requested_by"],
                "estimated_seconds": payload["estimated_seconds"],
                "power_milliwatts": power,
                "priority": payload["priority"],
                "payload": payload.get("payload", {}),
            }
        )
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT * FROM payload_tasks WHERE requested_by=? AND request_id=?",
                (payload["requested_by"], payload["request_id"]),
            ).fetchone()
            if existing is not None:
                if existing["request_fingerprint"] != fingerprint:
                    raise ConflictError("同一请求标识对应了不同的计算请求")
                return self._task_view(existing)
            cursor = connection.execute(
                "INSERT INTO payload_tasks(request_id,requested_by,request_fingerprint,payload_json,priority,estimated_seconds,remaining_seconds,power_milliwatts,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    payload["request_id"],
                    payload["requested_by"],
                    fingerprint,
                    json.dumps(payload.get("payload", {}), ensure_ascii=False, sort_keys=True),
                    payload["priority"],
                    payload["estimated_seconds"],
                    payload["estimated_seconds"],
                    power,
                    now,
                    now,
                ),
            )
            self._audit(connection, window_id=None, task_id=cursor.lastrowid, action="task.submitted", actor=payload["requested_by"], details={"request_id": payload["request_id"], "priority": payload["priority"], "estimated_seconds": payload["estimated_seconds"], "power_milliwatts": power}, now=now)
            row = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (cursor.lastrowid,)).fetchone()
            return self._task_view(row)

    def list_tasks(self, *, status: str | None = None, requested_by: str | None = None, window_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("status=?")
            values.append(status)
        if requested_by:
            clauses.append("requested_by=?")
            values.append(requested_by)
        if window_id is not None:
            clauses.append("window_id=?")
            values.append(window_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(max(1, min(limit, 500)))
        rows = self.connection.execute("SELECT * FROM payload_tasks" + where + " ORDER BY priority DESC, created_at ASC, id ASC LIMIT ?", values).fetchall()
        return [self._task_view(row) for row in rows]

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise NotFoundError("计算任务不存在")
        view = self._task_view(row)
        segments = self.connection.execute("SELECT * FROM payload_segments WHERE task_id=? ORDER BY id", (task_id,)).fetchall()
        view["segments"] = [self._segment_view(segment) for segment in segments]
        receipt = self.connection.execute("SELECT * FROM payload_receipts WHERE task_id=?", (task_id,)).fetchone()
        view["receipt"] = self._receipt_view(receipt) if receipt else None
        events = self.connection.execute("SELECT * FROM payload_audit_events WHERE task_id=? ORDER BY id", (task_id,)).fetchall()
        view["audit_events"] = [self._audit_view(event) for event in events]
        return view

    # ------------------------------------------------------------------
    # 排队与领取
    # ------------------------------------------------------------------
    def claim(self, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            window = self._open_window(connection, now)
            if window is None:
                return {"task": None, "reason": "no_open_window", "window": None, "must_pause_by": None, "lease_expires_at": None}
            window_view = self._window_view(connection, window, now)
            candidates = connection.execute("SELECT * FROM payload_tasks WHERE status IN ('queued','paused') ORDER BY priority DESC, created_at ASC, id ASC").fetchall()
            if not candidates:
                return {"task": None, "reason": "queue_empty", "window": window_view, "must_pause_by": None, "lease_expires_at": None}
            load = self._window_load(connection, window["id"])
            chosen: sqlite3.Row | None = None
            for candidate in candidates:
                if load["running_count"] >= window["max_concurrent_tasks"]:
                    break
                if load["running_power"] + candidate["power_milliwatts"] > window["max_power_milliwatts"]:
                    continue
                if window["energy_budget_millijoules"] > 0:
                    budget_left = window["energy_budget_millijoules"] - load["consumed_energy"] - load["in_flight_energy"]
                    if budget_left // candidate["power_milliwatts"] < 1:
                        continue
                chosen = candidate
                break
            if chosen is None:
                return {"task": None, "reason": "no_capacity", "window": window_view, "must_pause_by": None, "lease_expires_at": None}
            lease_expires_value = now_value + timedelta(seconds=lease_seconds)
            lease_expires = to_storage(lease_expires_value)
            cursor = connection.execute(
                "UPDATE payload_tasks SET status='running',window_id=?,lease_owner=?,lease_expires_at=?,segment_started_at=?,progress_seconds=0,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status IN ('queued','paused')",
                (window["id"], worker_id, lease_expires, now, now, now, chosen["id"]),
            )
            if cursor.rowcount != 1:
                return {"task": None, "reason": "claim_conflict", "window": window_view, "must_pause_by": None, "lease_expires_at": None}
            task = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (chosen["id"],)).fetchone()
            must_pause_by = self._must_pause_by(connection, window, task, now_value, lease_expires_value)
            action = "task.resumed" if chosen["status"] == "paused" else "task.claimed"
            self._audit(
                connection,
                window_id=window["id"],
                task_id=task["id"],
                action=action,
                actor=worker_id,
                details={
                    "previous_status": chosen["status"],
                    "priority": task["priority"],
                    "remaining_seconds": task["remaining_seconds"],
                    "pause_count": task["pause_count"],
                    "must_pause_by": to_storage(must_pause_by),
                },
                now=now,
            )
            return {
                "task": self._task_view(task),
                "reason": "claimed",
                "window": self._window_view(connection, window, now),
                "must_pause_by": to_storage(must_pause_by),
                "lease_expires_at": lease_expires,
            }

    def heartbeat(self, task_id: int, worker_id: str, progress_seconds: int, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            task = self._owned_task(connection, task_id, worker_id)
            accounted = self._checked_progress(task, progress_seconds)
            lease_expires_value = now_value + timedelta(seconds=lease_seconds)
            connection.execute(
                "UPDATE payload_tasks SET progress_seconds=?,lease_expires_at=?,updated_at=?,version=version+1 WHERE id=?",
                (accounted, to_storage(lease_expires_value), now, task_id),
            )
            updated = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
            window = connection.execute("SELECT * FROM payload_windows WHERE id=?", (updated["window_id"],)).fetchone()
            return self._execution_context(connection, updated, window, now_value, lease_expires_value)

    # ------------------------------------------------------------------
    # 暂停、恢复、完成
    # ------------------------------------------------------------------
    def pause(self, task_id: int, worker_id: str, end_reason: str, progress_seconds: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        with transaction(immediate=True) as connection:
            task = self._owned_task(connection, task_id, worker_id)
            return self._pause_task(connection, task, now_value, end_reason, worker_id, progress_seconds)

    def resume(self, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            task = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "paused":
                raise ConflictError("只有已暂停的任务可以重新排队")
            connection.execute("UPDATE payload_tasks SET status='queued',updated_at=?,version=version+1 WHERE id=?", (now, task_id))
            self._audit(connection, window_id=task["window_id"], task_id=task_id, action="task.requeued", actor=actor, details={"reason": reason, "priority": task["priority"], "remaining_seconds": task["remaining_seconds"]}, now=now)
            updated = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
            return self._task_view(updated)

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], progress_seconds: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            task = self._owned_task(connection, task_id, worker_id)
            reported = task["progress_seconds"] if progress_seconds is None else progress_seconds
            if reported < task["progress_seconds"]:
                raise ConflictError("完成回执的进度不可小于已上报进度")
            executed = max(0, min(reported, task["remaining_seconds"]))
            self._insert_segment(connection, task, now_value, reported, executed, "completed")
            totals = connection.execute("SELECT COALESCE(SUM(executed_seconds),0) AS seconds, COALESCE(SUM(energy_millijoules),0) AS energy FROM payload_segments WHERE task_id=?", (task_id,)).fetchone()
            digest = request_fingerprint(
                {
                    "task_id": task_id,
                    "request_id": task["request_id"],
                    "requested_by": task["requested_by"],
                    "result": result,
                    "metrics": metrics,
                    "total_executed_seconds": int(totals["seconds"]),
                    "total_energy_millijoules": int(totals["energy"]),
                }
            )
            cursor = connection.execute(
                "INSERT INTO payload_receipts(task_id,worker_id,result_json,metrics_json,total_executed_seconds,total_energy_millijoules,receipt_digest,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (task_id, worker_id, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), int(totals["seconds"]), int(totals["energy"]), digest, now),
            )
            connection.execute(
                "UPDATE payload_tasks SET status='completed',remaining_seconds=0,progress_seconds=0,lease_owner='',lease_expires_at='',segment_started_at=NULL,result_receipt_id=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (cursor.lastrowid, now, now, task_id),
            )
            self._audit(connection, window_id=task["window_id"], task_id=task_id, action="task.completed", actor=worker_id, details={"receipt_id": cursor.lastrowid, "receipt_digest": digest, "total_executed_seconds": int(totals["seconds"]), "total_energy_millijoules": int(totals["energy"])}, now=now)
            updated = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
            receipt = connection.execute("SELECT * FROM payload_receipts WHERE id=?", (cursor.lastrowid,)).fetchone()
            return {"task": self._task_view(updated), "receipt": self._receipt_view(receipt)}

    def cancel(self, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            task = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] not in {"queued", "paused"}:
                raise ConflictError("运行中的任务必须先安全暂停才能取消")
            connection.execute("UPDATE payload_tasks SET status='cancelled',finished_at=?,updated_at=?,version=version+1 WHERE id=?", (now, now, task_id))
            self._audit(connection, window_id=task["window_id"], task_id=task_id, action="task.cancelled", actor=actor, details={"reason": reason, "previous_status": task["status"], "remaining_seconds": task["remaining_seconds"]}, now=now)
            updated = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
            return self._task_view(updated)

    def get_receipt(self, task_id: int) -> dict[str, Any]:
        task = self.connection.execute("SELECT id FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise NotFoundError("计算任务不存在")
        receipt = self.connection.execute("SELECT * FROM payload_receipts WHERE task_id=?", (task_id,)).fetchone()
        if receipt is None:
            raise NotFoundError("任务尚未生成完成回执")
        return self._receipt_view(receipt)

    # ------------------------------------------------------------------
    # 核算、审计与仿真
    # ------------------------------------------------------------------
    def window_energy(self, window_id: int) -> dict[str, Any]:
        window = self.connection.execute("SELECT * FROM payload_windows WHERE id=?", (window_id,)).fetchone()
        if window is None:
            raise NotFoundError("热窗口不存在")
        load = self._window_load(self.connection, window_id)
        per_task = self.connection.execute(
            "SELECT task_id, COUNT(*) AS segments, COALESCE(SUM(executed_seconds),0) AS seconds, COALESCE(SUM(energy_millijoules),0) AS energy FROM payload_segments WHERE window_id=? GROUP BY task_id ORDER BY task_id",
            (window_id,),
        ).fetchall()
        budget = int(window["energy_budget_millijoules"])
        remaining_budget = budget - load["consumed_energy"] - load["in_flight_energy"] if budget > 0 else None
        return {
            "window_id": window_id,
            "window_key": window["window_key"],
            "energy_budget_millijoules": budget,
            "consumed_millijoules": load["consumed_energy"],
            "in_flight_millijoules": load["in_flight_energy"],
            "remaining_budget_millijoules": remaining_budget,
            "tasks": [
                {"task_id": int(row["task_id"]), "segments": int(row["segments"]), "executed_seconds": int(row["seconds"]), "energy_millijoules": int(row["energy"])}
                for row in per_task
            ],
        }

    def audit_events(
        self,
        *,
        task_id: int | None = None,
        window_id: int | None = None,
        action: str | None = None,
        actor: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        for column, value in (("task_id", task_id), ("window_id", window_id), ("action", action), ("actor", actor)):
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        if since:
            clauses.append("created_at>=?")
            values.append(since)
        if until:
            clauses.append("created_at<=?")
            values.append(until)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(max(1, min(limit, 500)))
        rows = self.connection.execute("SELECT * FROM payload_audit_events" + where + " ORDER BY id ASC LIMIT ?", values).fetchall()
        return [self._audit_view(row) for row in rows]

    def summary(self) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        states = {row["status"]: int(row["amount"]) for row in self.connection.execute("SELECT status,COUNT(*) AS amount FROM payload_tasks GROUP BY status").fetchall()}
        windows = {row["status"]: int(row["amount"]) for row in self.connection.execute("SELECT status,COUNT(*) AS amount FROM payload_windows GROUP BY status").fetchall()}
        open_window = self._open_window(self.connection, now)
        return {
            "tasks": states,
            "windows": windows,
            "open_window": self._window_view(self.connection, open_window, now) if open_window else None,
            "power_in_use_milliwatts": int(self.connection.execute("SELECT COALESCE(SUM(power_milliwatts),0) FROM payload_tasks WHERE status='running'").fetchone()[0]),
        }

    def plan(self, spec: dict[str, Any]) -> dict[str, Any]:
        """纯函数式排期仿真，不写数据库，API 与 CLI 结果一致。"""
        return simulate_spec(spec)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _open_window(self, connection: sqlite3.Connection, now: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM payload_windows WHERE status='scheduled' AND starts_at<=? AND ends_at>? ORDER BY starts_at, id LIMIT 1",
            (now, now),
        ).fetchone()

    def _window_load(self, connection: sqlite3.Connection, window_id: int) -> dict[str, int]:
        running = connection.execute(
            "SELECT COUNT(*) AS amount, COALESCE(SUM(power_milliwatts),0) AS power, COALESCE(SUM(progress_seconds*power_milliwatts),0) AS in_flight FROM payload_tasks WHERE window_id=? AND status='running'",
            (window_id,),
        ).fetchone()
        consumed = connection.execute("SELECT COALESCE(SUM(energy_millijoules),0) FROM payload_segments WHERE window_id=?", (window_id,)).fetchone()[0]
        return {
            "running_count": int(running["amount"]),
            "running_power": int(running["power"]),
            "in_flight_energy": int(running["in_flight"]),
            "consumed_energy": int(consumed),
        }

    def _must_pause_by(self, connection: sqlite3.Connection, window: sqlite3.Row, task: sqlite3.Row, now: datetime, lease_expires: datetime) -> datetime:
        candidates = [
            from_storage(window["ends_at"]) - timedelta(seconds=int(window["safe_pause_margin_seconds"])),
            lease_expires,
        ]
        budget = int(window["energy_budget_millijoules"])
        if budget > 0:
            load = self._window_load(connection, window["id"])
            budget_left = budget - load["consumed_energy"] - load["in_flight_energy"]
            affordable = max(0, budget_left // int(task["power_milliwatts"]))
            candidates.append(now + timedelta(seconds=affordable))
        return min(candidates)

    def _execution_context(self, connection: sqlite3.Connection, task: sqlite3.Row, window: sqlite3.Row | None, now: datetime, lease_expires: datetime) -> dict[str, Any]:
        if window is None:
            return {"task": self._task_view(task), "should_pause": True, "ready_to_complete": False, "must_pause_by": None, "window_remaining_seconds": 0, "budget_remaining_millijoules": None}
        must_pause_by = self._must_pause_by(connection, window, task, now, lease_expires)
        window_end = from_storage(window["ends_at"]) - timedelta(seconds=int(window["safe_pause_margin_seconds"]))
        window_remaining = max(0, int((window_end - now).total_seconds()))
        budget_remaining: int | None = None
        if int(window["energy_budget_millijoules"]) > 0:
            load = self._window_load(connection, window["id"])
            budget_remaining = int(window["energy_budget_millijoules"]) - load["consumed_energy"] - load["in_flight_energy"]
        ready = int(task["remaining_seconds"]) - int(task["progress_seconds"]) <= 0
        return {
            "task": self._task_view(task),
            "should_pause": now >= must_pause_by or window["status"] != "scheduled",
            "ready_to_complete": ready,
            "must_pause_by": to_storage(must_pause_by),
            "window_remaining_seconds": window_remaining,
            "budget_remaining_millijoules": budget_remaining,
        }

    def _owned_task(self, connection: sqlite3.Connection, task_id: int, worker_id: str) -> sqlite3.Row:
        task = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise NotFoundError("计算任务不存在")
        if task["status"] != "running" or task["lease_owner"] != worker_id:
            raise ConflictError("任务未由当前工作者持有")
        return task

    @staticmethod
    def _checked_progress(task: sqlite3.Row, progress_seconds: int) -> int:
        if progress_seconds < int(task["progress_seconds"]):
            raise ConflictError("任务进度不可回退")
        return min(progress_seconds, int(task["remaining_seconds"]))

    def _pause_running_tasks(self, connection: sqlite3.Connection, window_id: int, now: datetime, end_reason: str, actor: str) -> list[int]:
        running = connection.execute("SELECT * FROM payload_tasks WHERE window_id=? AND status='running' ORDER BY id", (window_id,)).fetchall()
        paused: list[int] = []
        for task in running:
            self._pause_task(connection, task, now, end_reason, actor, progress=None)
            paused.append(int(task["id"]))
        return paused

    def _pause_task(self, connection: sqlite3.Connection, task: sqlite3.Row, now: datetime, end_reason: str, actor: str, progress: int | None) -> dict[str, Any]:
        now_str = to_storage(now)
        reported = int(task["progress_seconds"]) if progress is None else progress
        if reported < int(task["progress_seconds"]):
            raise ConflictError("暂停时上报的进度不可小于已上报进度")
        executed = max(0, min(reported, int(task["remaining_seconds"])))
        self._insert_segment(connection, task, now, reported, executed, end_reason)
        remaining = int(task["remaining_seconds"]) - executed
        connection.execute(
            "UPDATE payload_tasks SET status='paused',remaining_seconds=?,progress_seconds=0,lease_owner='',lease_expires_at='',segment_started_at=NULL,pause_count=pause_count+1,last_pause_reason=?,updated_at=?,version=version+1 WHERE id=?",
            (remaining, end_reason, now_str, task["id"]),
        )
        self._audit(
            connection,
            window_id=task["window_id"],
            task_id=task["id"],
            action="task.paused",
            actor=actor,
            details={"end_reason": end_reason, "executed_seconds": executed, "remaining_seconds": remaining, "priority": task["priority"]},
            now=now_str,
        )
        updated = connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task["id"],)).fetchone()
        return self._task_view(updated)

    def _insert_segment(self, connection: sqlite3.Connection, task: sqlite3.Row, now: datetime, reported: int, executed: int, end_reason: str) -> None:
        now_str = to_storage(now)
        connection.execute(
            "INSERT INTO payload_segments(task_id,window_id,worker_id,started_at,ended_at,reported_seconds,executed_seconds,energy_millijoules,end_reason,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                task["id"],
                task["window_id"],
                task["lease_owner"],
                task["segment_started_at"] or task["started_at"] or now_str,
                now_str,
                reported,
                executed,
                executed * int(task["power_milliwatts"]),
                end_reason,
                now_str,
            ),
        )

    def _audit(self, connection: sqlite3.Connection, *, window_id: int | None, task_id: int | None, action: str, actor: str, details: dict[str, Any], now: str) -> None:
        connection.execute(
            "INSERT INTO payload_audit_events(task_id,window_id,action,actor,details_json,created_at) VALUES(?,?,?,?,?,?)",
            (task_id, window_id, action, actor, json.dumps(details, ensure_ascii=False, sort_keys=True), now),
        )

    # ------------------------------------------------------------------
    # 视图
    # ------------------------------------------------------------------
    def _window_view(self, connection: sqlite3.Connection, row: sqlite3.Row, now: str) -> dict[str, Any]:
        view = dict(row)
        view.pop("request_fingerprint", None)
        load = self._window_load(connection, row["id"])
        starts_at = from_storage(row["starts_at"])
        ends_at = from_storage(row["ends_at"])
        current = from_storage(now)
        is_open = row["status"] == "scheduled" and starts_at <= current < ends_at
        budget = int(row["energy_budget_millijoules"])
        view.update(
            {
                "max_power_watts": row["max_power_milliwatts"] / 1000,
                "energy_budget_joules": budget / 1000,
                "is_open": is_open,
                "remaining_seconds": max(0, int((ends_at - current).total_seconds())),
                "running_tasks": load["running_count"],
                "running_power_milliwatts": load["running_power"],
                "consumed_energy_millijoules": load["consumed_energy"],
                "in_flight_energy_millijoules": load["in_flight_energy"],
                "remaining_energy_budget_millijoules": (budget - load["consumed_energy"] - load["in_flight_energy"]) if budget > 0 else None,
            }
        )
        return view

    @staticmethod
    def _task_view(row: sqlite3.Row) -> dict[str, Any]:
        view = dict(row)
        view["payload"] = json.loads(view.pop("payload_json"))
        view.pop("request_fingerprint", None)
        view["power_watts"] = row["power_milliwatts"] / 1000
        view["executed_seconds"] = int(row["estimated_seconds"]) - int(row["remaining_seconds"]) - int(row["progress_seconds"])
        return view

    @staticmethod
    def _segment_view(row: sqlite3.Row) -> dict[str, Any]:
        view = dict(row)
        view["energy_joules"] = row["energy_millijoules"] / 1000
        return view

    @staticmethod
    def _receipt_view(row: sqlite3.Row) -> dict[str, Any]:
        view = dict(row)
        view["result"] = json.loads(view.pop("result_json"))
        view["metrics"] = json.loads(view.pop("metrics_json"))
        view["total_energy_joules"] = row["total_energy_millijoules"] / 1000
        return view

    @staticmethod
    def _audit_view(row: sqlite3.Row) -> dict[str, Any]:
        view = dict(row)
        view["details"] = json.loads(view.pop("details_json"))
        return view
