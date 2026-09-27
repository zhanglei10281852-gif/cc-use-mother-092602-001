from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError
from app.database import get_connection, transaction
from app.payload.repository import PayloadRepository

ENERGY_DIGITS = 6


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def joules(power_watts: float, seconds: int | float) -> float:
    """唯一的能耗计算公式：焦耳 = 瓦特 × 秒，保留 6 位小数保证可复现。"""
    return round(float(power_watts) * float(seconds), ENERGY_DIGITS)


def whole_seconds(start: datetime, end: datetime) -> int:
    return max(0, int((end - start).total_seconds()))


def as_utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class PayloadService:
    """管理卫星载荷的热窗口、计算任务排队领取、安全暂停恢复、完成回执与审计。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = PayloadRepository(self.connection)

    # ---------- 热窗口 ----------

    def create_window(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        starts_at = to_storage(as_utc(payload["starts_at"]))
        ends_at = to_storage(as_utc(payload["ends_at"]))
        if ends_at <= starts_at:
            raise ConflictError("热窗口结束时间必须晚于开始时间")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PayloadRepository(connection)
            if repository.window_by_name(payload["name"]):
                raise ConflictError("热窗口名称已存在")
            overlap = repository.overlapping_window(starts_at, ends_at)
            if overlap is not None:
                raise ConflictError("热窗口与已有窗口时间重叠", context={"overlap_with": overlap["name"]})
            window = repository.create_window(
                name=payload["name"], starts_at=starts_at, ends_at=ends_at,
                energy_budget_joules=float(payload["energy_budget_joules"]),
                max_power_watts=float(payload["max_power_watts"]),
                created_by=actor, now=now,
            )
            repository.add_event(
                event_type="window_created", actor=actor, window_id=window["id"], now=now,
                details={"name": window["name"], "starts_at": starts_at, "ends_at": ends_at,
                         "energy_budget_joules": window["energy_budget_joules"], "max_power_watts": window["max_power_watts"]},
            )
            return self._window_view(window, now)

    def list_windows(self) -> list[dict[str, Any]]:
        now = to_storage(self.clock.now())
        return [self._window_view(window, now) for window in self.repository.list_windows()]

    def get_window(self, window_id: int) -> dict[str, Any]:
        window = self.repository.window_by_id(window_id)
        if window is None:
            raise NotFoundError("热窗口不存在")
        now = to_storage(self.clock.now())
        view = self._window_view(dict(window), now)
        view["running_tasks"] = len(self.repository.running_tasks_in_window(window_id))
        return view

    def pause_window_tasks(self, window_id: int, actor: str, reason: str) -> dict[str, Any]:
        """窗口即将结束时安全暂停全部在运行任务，保留原始优先级与剩余工作量。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PayloadRepository(connection)
            window = repository.window_by_id(window_id)
            if window is None:
                raise NotFoundError("热窗口不存在")
            window_end = from_storage(window["ends_at"])
            paused: list[dict[str, Any]] = []
            for task in repository.running_tasks_in_window(window_id):
                elapsed = self._billable_seconds(task, now_value, window_end)
                energy = joules(task["power_watts"], elapsed)
                remaining = int(task["remaining_seconds"]) - elapsed
                connection.execute(
                    "UPDATE payload_tasks SET status='paused',remaining_seconds=?,total_run_seconds=total_run_seconds+?,energy_joules=energy_joules+?,pause_count=pause_count+1,worker_id='',updated_at=?,version=version+1 WHERE id=? AND status='running'",
                    (remaining, elapsed, energy, now, task["id"]),
                )
                repository.add_window_energy(window_id, energy)
                after = dict(repository.task_by_id(task["id"]))
                repository.add_event(
                    event_type="task_paused", actor=actor, task_id=task["id"], window_id=window_id, now=now,
                    details={"reason": reason, "elapsed_seconds": elapsed, "remaining_seconds": remaining,
                             "energy_joules": energy, "priority": after["priority"]},
                )
                paused.append(after)
            repository.add_event(
                event_type="window_paused", actor=actor, window_id=window_id, now=now,
                details={"reason": reason, "paused_task_ids": [task["id"] for task in paused]},
            )
            return {"window_id": window_id, "paused": paused}

    # ---------- 任务提交与领取 ----------

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        request_digest = digest({
            "name": payload["name"], "estimated_seconds": payload["estimated_seconds"],
            "power_watts": payload["power_watts"], "priority": payload["priority"],
        })
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PayloadRepository(connection)
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            if existing is not None:
                if existing["request_digest"] != request_digest:
                    raise ConflictError("同一幂等键对应了不同的计算请求")
                task = dict(existing)
                task["idempotent_replay"] = True
                return task
            task = repository.create_task(
                name=payload["name"], requested_by=payload["requested_by"],
                idempotency_key=payload["idempotency_key"], request_digest=request_digest,
                estimated_seconds=payload["estimated_seconds"], power_watts=float(payload["power_watts"]),
                priority=payload["priority"], now=now,
            )
            repository.add_event(
                event_type="task_submitted", actor=payload["requested_by"], task_id=task["id"], now=now,
                details={"name": task["name"], "estimated_seconds": task["estimated_seconds"],
                         "power_watts": task["power_watts"], "priority": task["priority"]},
            )
            task["idempotent_replay"] = False
            return task

    def list_tasks(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        task = self.repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("载荷任务不存在")
        result = dict(task)
        receipt = self.repository.receipt_by_task(task_id)
        result["receipt"] = dict(receipt) if receipt else None
        result["events"] = self.repository.list_events(task_id=task_id, window_id=None, event_type=None, limit=500)
        return result

    def claim(self, worker_id: str) -> dict[str, Any]:
        """在温度预算允许时按原始优先级领取任务；暂停任务以剩余工作量恢复。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PayloadRepository(connection)
            window = repository.active_window(now)
            if window is None:
                return {"task": None, "reason": "no_active_window"}
            energy_left = float(window["energy_budget_joules"]) - float(window["energy_used_joules"])
            if energy_left <= 0:
                return {"task": None, "reason": "window_energy_exhausted"}
            candidate = repository.claim_candidate(min(float(window["max_power_watts"]), energy_left))
            if candidate is None:
                return {"task": None, "reason": "no_eligible_task"}
            cursor = connection.execute(
                "UPDATE payload_tasks SET status='running',window_id=?,worker_id=?,last_resumed_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status IN ('queued','paused')",
                (window["id"], worker_id, now, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return {"task": None, "reason": "claim_race_lost"}
            task = dict(repository.task_by_id(candidate["id"]))
            repository.add_event(
                event_type="task_resumed" if candidate["status"] == "paused" else "task_claimed",
                actor=worker_id, task_id=task["id"], window_id=window["id"], now=now,
                details={"remaining_seconds": task["remaining_seconds"], "priority": task["priority"],
                         "pause_count": task["pause_count"]},
            )
            return {"task": task, "reason": None, "window": self._window_view(dict(repository.window_by_id(window["id"])), now)}

    # ---------- 完成回执 ----------

    def complete(self, task_id: int, worker_id: str, result_summary: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PayloadRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("载荷任务不存在")
            existing = repository.receipt_by_task(task_id)
            if task["status"] == "completed" and existing is not None:
                return {"task": dict(task), "receipt": dict(existing), "idempotent_replay": True}
            if task["status"] != "running" or task["worker_id"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            window = repository.window_by_id(task["window_id"]) if task["window_id"] else None
            window_end = from_storage(window["ends_at"]) if window else now_value
            elapsed = self._billable_seconds(task, now_value, window_end)
            energy = joules(task["power_watts"], elapsed)
            connection.execute(
                "UPDATE payload_tasks SET status='completed',remaining_seconds=0,total_run_seconds=total_run_seconds+?,energy_joules=energy_joules+?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (elapsed, energy, now, now, task_id),
            )
            if window is not None:
                repository.add_window_energy(window["id"], energy)
            after = dict(repository.task_by_id(task_id))
            receipt = repository.create_receipt(
                task_id=task_id, worker_id=worker_id,
                total_run_seconds=after["total_run_seconds"], total_energy_joules=after["energy_joules"],
                windows_used=repository.count_task_windows(task_id),
                result_summary=result_summary, now=now,
            )
            repository.add_event(
                event_type="task_completed", actor=worker_id, task_id=task_id,
                window_id=window["id"] if window else None, now=now,
                details={"elapsed_seconds": elapsed, "energy_joules": energy,
                         "total_run_seconds": receipt["total_run_seconds"],
                         "total_energy_joules": receipt["total_energy_joules"],
                         "windows_used": receipt["windows_used"]},
            )
            return {"task": after, "receipt": receipt, "idempotent_replay": False}

    # ---------- 审计与复现 ----------

    def audit(self, *, task_id: int | None = None, window_id: int | None = None, event_type: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return self.repository.list_events(task_id=task_id, window_id=window_id, event_type=event_type, limit=max(1, min(limit, 1000)))

    def plan(self, at: datetime | None = None) -> dict[str, Any]:
        """按当前队列与窗口配置推演调度计划；相同输入与 at 必然得到相同输出。"""
        moment = as_utc(at) if at is not None else self.clock.now()
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        now_iso = to_storage(moment)
        windows = [w for w in self.repository.list_windows() if w["ends_at"] > now_iso]
        tasks = self.repository.plannable_tasks()
        window_state: dict[int, dict[str, Any]] = {}
        for window in windows:
            start = max(from_storage(window["starts_at"]), moment)
            window_state[window["id"]] = {
                "window": window, "cursor": start, "consumed": 0.0,
                "energy_left": max(0.0, float(window["energy_budget_joules"]) - float(window["energy_used_joules"])),
            }
        effective_remaining: dict[int, int] = {}
        for task in tasks:
            remaining = int(task["remaining_seconds"])
            if task["status"] == "running" and task["last_resumed_at"] and task["window_id"] in window_state:
                state = window_state[task["window_id"]]
                settled = whole_seconds(from_storage(task["last_resumed_at"]), min(moment, from_storage(state["window"]["ends_at"])))
                settled = min(settled, remaining)
                remaining -= settled
                energy = joules(task["power_watts"], settled)
                state["energy_left"] = max(0.0, state["energy_left"] - energy)
                state["consumed"] += energy
            effective_remaining[task["id"]] = remaining
        planned_windows: dict[int, dict[str, Any]] = {}
        planned_tasks: list[dict[str, Any]] = []
        unscheduled: list[dict[str, Any]] = []
        for task in tasks:
            remaining = effective_remaining[task["id"]]
            segments: list[dict[str, Any]] = []
            for state in window_state.values():
                if remaining <= 0:
                    break
                window = state["window"]
                if float(task["power_watts"]) > float(window["max_power_watts"]):
                    continue
                end = from_storage(window["ends_at"])
                time_left = whole_seconds(state["cursor"], end)
                energy_left = state["energy_left"]
                if time_left <= 0 or energy_left <= 0:
                    continue
                run = min(remaining, time_left, int(energy_left / float(task["power_watts"])))
                if run <= 0:
                    continue
                seg_start = state["cursor"]
                seg_end = seg_start + timedelta(seconds=run)
                energy = joules(task["power_watts"], run)
                segments.append({"window_id": window["id"], "start": to_storage(seg_start),
                                 "end": to_storage(seg_end), "seconds": run, "energy_joules": energy})
                state["cursor"] = seg_end
                state["energy_left"] = round(energy_left - energy, ENERGY_DIGITS)
                state["consumed"] = round(state["consumed"] + energy, ENERGY_DIGITS)
                remaining -= run
            entry = {"task_id": task["id"], "name": task["name"], "status": task["status"],
                     "priority": task["priority"], "power_watts": task["power_watts"],
                     "remaining_seconds": effective_remaining[task["id"]],
                     "scheduled_seconds": sum(segment["seconds"] for segment in segments),
                     "segments": segments,
                     "completes_at": segments[-1]["end"] if segments and remaining <= 0 else None}
            if segments:
                planned_tasks.append(entry)
            else:
                limit_power = max((float(w["max_power_watts"]) for w in windows), default=0.0)
                reason = "power_exceeds_window_limits" if float(task["power_watts"]) > limit_power else "insufficient_window_capacity"
                unscheduled.append({"task_id": task["id"], "name": task["name"], "reason": reason})
        for window in windows:
            state = window_state[window["id"]]
            planned_windows[window["id"]] = {
                "window_id": window["id"], "name": window["name"],
                "starts_at": window["starts_at"], "ends_at": window["ends_at"],
                "energy_budget_joules": window["energy_budget_joules"],
                "energy_used_joules": window["energy_used_joules"],
                "energy_remaining_joules": round(state["energy_left"], ENERGY_DIGITS),
                "planned_energy_joules": round(state["consumed"], ENERGY_DIGITS),
                "planned_busy_seconds": whole_seconds(max(from_storage(window["starts_at"]), moment), state["cursor"]),
            }
        return {"generated_at": now_iso, "windows": list(planned_windows.values()),
                "tasks": planned_tasks, "unscheduled": unscheduled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM payload_tasks GROUP BY status ORDER BY status").fetchall()
        now = to_storage(self.clock.now())
        active = self.repository.active_window(now)
        return {
            "states": {row["status"]: row["amount"] for row in rows},
            "active_window": self._window_view(dict(active), now) if active else None,
            "windows": len(self.repository.list_windows()),
        }

    # ---------- 内部工具 ----------

    @staticmethod
    def _billable_seconds(task: sqlite3.Row, now: datetime, window_end: datetime | None) -> int:
        """任务本次运行应计费的秒数：不早于上次恢复时间，不晚于窗口结束，且不超过剩余工作量。"""
        start = from_storage(task["last_resumed_at"]) or now
        end = min(now, window_end) if window_end else now
        return min(whole_seconds(start, end), int(task["remaining_seconds"]))

    @staticmethod
    def _window_view(window: dict[str, Any], now: str) -> dict[str, Any]:
        view = dict(window)
        if now < view["starts_at"]:
            status = "scheduled"
        elif now < view["ends_at"]:
            status = "active"
        else:
            status = "closed"
        view["status"] = status
        view["energy_remaining_joules"] = round(float(view["energy_budget_joules"]) - float(view["energy_used_joules"]), ENERGY_DIGITS)
        return view
