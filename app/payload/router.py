from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Query

from app.payload.schemas import TaskClaim, TaskComplete, TaskSubmit, WindowCreate, WindowPause
from app.payload.service import PayloadService

router = APIRouter(prefix="/api/payload", tags=["卫星载荷热窗口调度"])


def service() -> PayloadService:
    return PayloadService()


@router.post("/windows", status_code=201)
def create_window(payload: WindowCreate, actor: str = Query(..., min_length=1)):
    return service().create_window(payload.model_dump(), actor)


@router.get("/windows")
def list_windows():
    return {"items": service().list_windows()}


@router.get("/windows/{window_id}")
def get_window(window_id: int):
    return service().get_window(window_id)


@router.post("/windows/{window_id}/pause")
def pause_window(window_id: int, payload: WindowPause):
    return service().pause_window_tasks(window_id, payload.actor, payload.reason)


@router.post("/tasks", status_code=202)
def submit_task(payload: TaskSubmit):
    return service().submit(payload.model_dump())


@router.get("/tasks")
def list_tasks(status: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_tasks(status=status, limit=limit)}


@router.post("/tasks/claim")
def claim_task(payload: TaskClaim):
    return service().claim(payload.worker_id)


@router.get("/tasks/{task_id}")
def get_task(task_id: int):
    return service().get_task(task_id)


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskComplete):
    return service().complete(task_id, payload.worker_id, payload.result_summary)


@router.get("/audit")
def audit(task_id: int | None = None, window_id: int | None = None, event_type: str | None = None, limit: int = Query(default=200, ge=1, le=1000)):
    return {"items": service().audit(task_id=task_id, window_id=window_id, event_type=event_type, limit=limit)}


@router.get("/plan")
def plan(at: datetime | None = None):
    return service().plan(at)


@router.get("/summary")
def summary():
    return service().summary()
