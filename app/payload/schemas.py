from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class WindowConfigure(BaseModel):
    window_key: str = Field(min_length=3, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]+$")
    name: str = Field(min_length=2, max_length=120)
    starts_at: datetime
    ends_at: datetime
    max_power_watts: float = Field(gt=0, le=1_000_000)
    max_concurrent_tasks: int = Field(default=1, ge=1, le=64)
    energy_budget_joules: float = Field(default=0, ge=0, le=1_000_000_000)
    safe_pause_margin_seconds: int = Field(default=0, ge=0, le=3600)

    @model_validator(mode="after")
    def validate_window(self) -> "WindowConfigure":
        if self.ends_at <= self.starts_at:
            raise ValueError("窗口结束时间必须晚于开始时间")
        length = (self.ends_at - self.starts_at).total_seconds()
        if self.safe_pause_margin_seconds >= length:
            raise ValueError("安全暂停余量必须小于窗口长度")
        return self


class WindowClose(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class TaskSubmit(BaseModel):
    request_id: str = Field(min_length=3, max_length=160)
    requested_by: str = Field(min_length=1, max_length=80)
    estimated_seconds: int = Field(gt=0, le=7 * 24 * 3600)
    power_watts: float = Field(gt=0, le=100_000)
    priority: int = Field(default=50, ge=0, le=100)
    payload: dict[str, Any] = Field(default_factory=dict)


class TaskClaim(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class TaskHeartbeat(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    progress_seconds: int = Field(ge=0, le=7 * 24 * 3600)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class TaskPause(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    progress_seconds: int | None = Field(default=None, ge=0, le=7 * 24 * 3600)
    end_reason: Literal["window_end", "operator_pause", "budget_exhausted"] = "operator_pause"


class TaskComplete(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    result: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    progress_seconds: int | None = Field(default=None, ge=0, le=7 * 24 * 3600)


class TaskResume(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class TaskCancel(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class PlanTaskSpec(BaseModel):
    task_key: str = Field(min_length=1, max_length=160)
    priority: int = Field(default=50, ge=0, le=100)
    remaining_seconds: int = Field(gt=0, le=7 * 24 * 3600)
    power_watts: float = Field(gt=0, le=100_000)


class PlanWindowSpec(BaseModel):
    window_key: str = Field(min_length=1, max_length=120)
    starts_at: datetime
    ends_at: datetime
    max_power_watts: float = Field(gt=0, le=1_000_000)
    max_concurrent_tasks: int = Field(default=1, ge=1, le=64)
    energy_budget_joules: float = Field(default=0, ge=0, le=1_000_000_000)
    safe_pause_margin_seconds: int = Field(default=0, ge=0, le=3600)

    @model_validator(mode="after")
    def validate_window(self) -> "PlanWindowSpec":
        if self.ends_at <= self.starts_at:
            raise ValueError("窗口结束时间必须晚于开始时间")
        return self


class PlanRequest(BaseModel):
    now: datetime | None = None
    windows: list[PlanWindowSpec] = Field(min_length=1, max_length=200)
    tasks: list[PlanTaskSpec] = Field(min_length=1, max_length=500)
