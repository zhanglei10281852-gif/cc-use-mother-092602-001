"""确定性热窗口调度仿真与单位换算。

所有时间使用整数秒、功率使用整数毫瓦、能量使用整数毫焦，
相同输入在 API 与命令行下必然产生逐字节一致的结果。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.clock import to_storage
from app.core.errors import ValidationError


def watts_to_milliwatts(value: float) -> int:
    """瓦特换算为整数毫瓦，最多保留三位小数。"""
    return int(round(float(value) * 1000))


def joules_to_millijoules(value: float) -> int:
    """焦耳换算为整数毫焦。"""
    return int(round(float(value) * 1000))


def parse_instant(value: Any) -> datetime:
    """把 ISO 字符串或 datetime 统一为带 UTC 时区的整秒时间。"""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.strip())
    else:
        raise ValidationError("时间必须是 ISO 8601 字符串")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


@dataclass(frozen=True, slots=True)
class PlanTask:
    task_key: str
    priority: int
    remaining_seconds: int
    power_milliwatts: int


@dataclass(frozen=True, slots=True)
class PlanWindow:
    window_key: str
    starts_at: datetime
    ends_at: datetime
    max_power_milliwatts: int
    max_concurrent_tasks: int
    safe_pause_margin_seconds: int
    energy_budget_millijoules: int  # 0 表示不限制


def simulate(tasks: list[PlanTask], windows: list[PlanWindow], now: datetime) -> dict[str, Any]:
    """事件推进式贪心仿真：按优先级填充窗口容量，窗口结束即安全暂停。

    任务在窗口边界被暂停后保留剩余工作量，在后续窗口按原始优先级继续，
    与在线调度（claim/pause/resume）使用同一套约束规则。
    """
    remaining = {task.task_key: task.remaining_seconds for task in tasks}
    energy = {task.task_key: 0 for task in tasks}
    first_started: dict[str, datetime] = {}
    finished_at: dict[str, datetime] = {}
    ran_once: set[str] = set()
    order = {task.task_key: index for index, task in enumerate(tasks)}
    by_key = {task.task_key: task for task in tasks}
    segments: list[dict[str, Any]] = []
    ordered_windows = sorted(windows, key=lambda window: (window.starts_at, window.window_key))
    window_stats: dict[str, dict[str, int]] = {
        window.window_key: {"task_seconds": 0, "energy_millijoules": 0, "segments": 0} for window in ordered_windows
    }

    for window in ordered_windows:
        usable_start = max(window.starts_at, now)
        usable_end = window.ends_at - timedelta(seconds=window.safe_pause_margin_seconds)
        if usable_end <= usable_start:
            continue
        unlimited = window.energy_budget_millijoules <= 0
        budget_left = window.energy_budget_millijoules
        running: dict[str, datetime] = {}
        current = usable_start

        def close_segment(task_key: str, end: datetime, reason: str) -> None:
            start = running.pop(task_key)
            seconds = int((end - start).total_seconds())
            if seconds <= 0:
                return
            used = seconds * by_key[task_key].power_milliwatts
            segments.append(
                {
                    "task_key": task_key,
                    "window_key": window.window_key,
                    "starts_at": to_storage(start),
                    "ends_at": to_storage(end),
                    "seconds": seconds,
                    "energy_millijoules": used,
                    "end_reason": reason,
                }
            )
            stats = window_stats[window.window_key]
            stats["task_seconds"] += seconds
            stats["energy_millijoules"] += used
            stats["segments"] += 1

        while True:
            while len(running) < window.max_concurrent_tasks and (unlimited or budget_left > 0):
                used_power = sum(by_key[key].power_milliwatts for key in running)
                candidates = [
                    task
                    for task in tasks
                    if remaining[task.task_key] > 0
                    and task.task_key not in running
                    and used_power + task.power_milliwatts <= window.max_power_milliwatts
                ]
                if not candidates:
                    break
                chosen = min(candidates, key=lambda task: (-task.priority, order[task.task_key]))
                running[chosen.task_key] = current
                ran_once.add(chosen.task_key)
                first_started.setdefault(chosen.task_key, current)
            if not running:
                break
            soonest_completion = min(remaining[key] for key in running)
            step = min(soonest_completion, int((usable_end - current).total_seconds()))
            if not unlimited:
                total_power = sum(by_key[key].power_milliwatts for key in running)
                step = min(step, budget_left // total_power)
            if step <= 0:
                reason = "window_end" if current >= usable_end else "budget_exhausted"
                for key in list(running):
                    close_segment(key, current, reason)
                break
            for key in running:
                remaining[key] -= step
                energy[key] += step * by_key[key].power_milliwatts
            if not unlimited:
                budget_left -= step * sum(by_key[key].power_milliwatts for key in running)
            current = current + timedelta(seconds=step)
            for key in [key for key in running if remaining[key] == 0]:
                finished_at[key] = current
                close_segment(key, current, "completed")
            if current >= usable_end or (not unlimited and budget_left <= 0):
                reason = "window_end" if current >= usable_end else "budget_exhausted"
                for key in list(running):
                    close_segment(key, current, reason)
                break

    task_summaries: list[dict[str, Any]] = []
    for task in tasks:
        key = task.task_key
        done = remaining[key] == 0
        unscheduled_reason = ""
        if not done:
            if key not in ran_once and all(task.power_milliwatts > window.max_power_milliwatts for window in windows):
                unscheduled_reason = "power_exceeds_window_capacity"
            else:
                unscheduled_reason = "insufficient_window_budget"
        task_summaries.append(
            {
                "task_key": key,
                "priority": task.priority,
                "requested_seconds": task.remaining_seconds,
                "executed_seconds": task.remaining_seconds - remaining[key],
                "remaining_seconds": remaining[key],
                "energy_millijoules": energy[key],
                "status": "completed" if done else "unfinished",
                "unscheduled_reason": unscheduled_reason,
                "first_started_at": to_storage(first_started[key]) if key in first_started else None,
                "finished_at": to_storage(finished_at[key]) if key in finished_at else None,
            }
        )
    return {
        "generated_at": to_storage(now),
        "segments": segments,
        "tasks": task_summaries,
        "windows": [{"window_key": window.window_key, **window_stats[window.window_key]} for window in ordered_windows],
        "totals": {
            "energy_millijoules": sum(energy.values()),
            "executed_seconds": sum(task.remaining_seconds - remaining[task.task_key] for task in tasks),
            "completed_tasks": sum(1 for task in tasks if remaining[task.task_key] == 0),
            "unfinished_tasks": sum(1 for task in tasks if remaining[task.task_key] > 0),
        },
    }


def task_from_spec(spec: dict[str, Any]) -> PlanTask:
    power = watts_to_milliwatts(spec["power_watts"])
    if power < 1:
        raise ValidationError("任务功率换算后不足 1 毫瓦")
    return PlanTask(
        task_key=str(spec["task_key"]),
        priority=int(spec.get("priority", 50)),
        remaining_seconds=int(spec["remaining_seconds"]),
        power_milliwatts=power,
    )


def window_from_spec(spec: dict[str, Any]) -> PlanWindow:
    max_power = watts_to_milliwatts(spec["max_power_watts"])
    if max_power < 1:
        raise ValidationError("窗口功率上限换算后不足 1 毫瓦")
    starts_at = parse_instant(spec["starts_at"])
    ends_at = parse_instant(spec["ends_at"])
    if ends_at <= starts_at:
        raise ValidationError("窗口结束时间必须晚于开始时间")
    return PlanWindow(
        window_key=str(spec["window_key"]),
        starts_at=starts_at,
        ends_at=ends_at,
        max_power_milliwatts=max_power,
        max_concurrent_tasks=int(spec.get("max_concurrent_tasks", 1)),
        safe_pause_margin_seconds=int(spec.get("safe_pause_margin_seconds", 0)),
        energy_budget_millijoules=joules_to_millijoules(spec.get("energy_budget_joules", 0)),
    )


def simulate_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """从 API/CLI 共用的字典规格执行仿真，保证两条路径结果一致。"""
    windows = [window_from_spec(item) for item in spec["windows"]]
    tasks = [task_from_spec(item) for item in spec["tasks"]]
    if spec.get("now"):
        now = parse_instant(spec["now"])
    else:
        now = min(window.starts_at for window in windows)
    return simulate(tasks, windows, now)
