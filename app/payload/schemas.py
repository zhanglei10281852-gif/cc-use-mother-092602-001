from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class WindowCreate(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    starts_at: datetime
    ends_at: datetime
    energy_budget_joules: float = Field(gt=0, le=1e12)
    max_power_watts: float = Field(gt=0, le=100000)

    @model_validator(mode="after")
    def validate_range(self) -> "WindowCreate":
        if self.ends_at <= self.starts_at:
            raise ValueError("热窗口结束时间必须晚于开始时间")
        return self


class TaskSubmit(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    requested_by: str = Field(min_length=1, max_length=80)
    idempotency_key: str = Field(min_length=6, max_length=160)
    estimated_seconds: int = Field(gt=0, le=86400)
    power_watts: float = Field(gt=0, le=100000)
    priority: int = Field(default=50, ge=0, le=100)


class TaskClaim(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)


class TaskComplete(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    result_summary: str = Field(default="", max_length=2000)


class WindowPause(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
