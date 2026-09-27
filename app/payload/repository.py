from __future__ import annotations

import json
import sqlite3
from typing import Any


class PayloadRepository:
    """封装卫星载荷热窗口调度领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def window_by_id(self, window_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM payload_windows WHERE id=?", (window_id,)).fetchone()

    def window_by_name(self, name: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM payload_windows WHERE name=?", (name,)).fetchone()

    def overlapping_window(self, starts_at: str, ends_at: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM payload_windows WHERE starts_at<? AND ends_at>? ORDER BY starts_at LIMIT 1",
            (ends_at, starts_at),
        ).fetchone()

    def list_windows(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM payload_windows ORDER BY starts_at,id").fetchall()
        return [dict(row) for row in rows]

    def create_window(self, *, name: str, starts_at: str, ends_at: str, energy_budget_joules: float, max_power_watts: float, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO payload_windows(name,starts_at,ends_at,energy_budget_joules,max_power_watts,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
            (name, starts_at, ends_at, energy_budget_joules, max_power_watts, created_by, now),
        )
        return dict(self.window_by_id(cursor.lastrowid))

    def active_window(self, now: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM payload_windows WHERE starts_at<=? AND ends_at>? ORDER BY starts_at LIMIT 1",
            (now, now),
        ).fetchone()

    def add_window_energy(self, window_id: int, energy_joules: float) -> None:
        self.connection.execute(
            "UPDATE payload_windows SET energy_used_joules=energy_used_joules+? WHERE id=?",
            (energy_joules, window_id),
        )

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM payload_tasks WHERE id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM payload_tasks WHERE requested_by=? AND idempotency_key=?",
            (requested_by, key),
        ).fetchone()

    def create_task(self, *, name: str, requested_by: str, idempotency_key: str, request_digest: str, estimated_seconds: int, power_watts: float, priority: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO payload_tasks(name,requested_by,idempotency_key,request_digest,estimated_seconds,remaining_seconds,power_watts,priority,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'queued',?,?)",
            (name, requested_by, idempotency_key, request_digest, estimated_seconds, estimated_seconds, power_watts, priority, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def list_tasks(self, *, status: str | None, limit: int) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute(
                "SELECT * FROM payload_tasks WHERE status=? ORDER BY priority DESC,created_at ASC,id ASC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM payload_tasks ORDER BY priority DESC,created_at ASC,id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_candidate(self, max_power_watts: float) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM payload_tasks WHERE status IN ('queued','paused') AND remaining_seconds>0 AND power_watts<=? ORDER BY priority DESC,created_at ASC,id ASC LIMIT 1",
            (max_power_watts,),
        ).fetchone()

    def running_tasks_in_window(self, window_id: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM payload_tasks WHERE window_id=? AND status='running' ORDER BY id",
            (window_id,),
        ).fetchall()

    def plannable_tasks(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM payload_tasks WHERE status IN ('queued','paused','running') AND remaining_seconds>0 ORDER BY priority DESC,created_at ASC,id ASC"
        ).fetchall()
        return [dict(row) for row in rows]

    def receipt_by_task(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM payload_receipts WHERE task_id=?", (task_id,)).fetchone()

    def create_receipt(self, *, task_id: int, worker_id: str, total_run_seconds: int, total_energy_joules: float, windows_used: int, result_summary: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO payload_receipts(task_id,worker_id,total_run_seconds,total_energy_joules,windows_used,result_summary,completed_at) VALUES(?,?,?,?,?,?,?)",
            (task_id, worker_id, total_run_seconds, total_energy_joules, windows_used, result_summary, now),
        )
        return dict(self.connection.execute("SELECT * FROM payload_receipts WHERE id=?", (cursor.lastrowid,)).fetchone())

    def count_task_windows(self, task_id: int) -> int:
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM payload_events WHERE task_id=? AND event_type IN ('task_claimed','task_resumed')",
                (task_id,),
            ).fetchone()[0]
        )

    def add_event(self, *, event_type: str, actor: str, task_id: int | None = None, window_id: int | None = None, details: dict[str, Any] | None = None, now: str) -> None:
        self.connection.execute(
            "INSERT INTO payload_events(event_type,task_id,window_id,actor,details_json,created_at) VALUES(?,?,?,?,?,?)",
            (event_type, task_id, window_id, actor, json.dumps(details or {}, ensure_ascii=False, sort_keys=True), now),
        )

    def list_events(self, *, task_id: int | None, window_id: int | None, event_type: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if task_id is not None:
            clauses.append("task_id=?")
            values.append(task_id)
        if window_id is not None:
            clauses.append("window_id=?")
            values.append(window_id)
        if event_type:
            clauses.append("event_type=?")
            values.append(event_type)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT * FROM payload_events" + where + " ORDER BY id LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]
