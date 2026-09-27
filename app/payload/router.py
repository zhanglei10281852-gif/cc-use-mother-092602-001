from __future__ import annotations

from fastapi import APIRouter, Query

from app.payload.schemas import (
    PlanRequest,
    TaskCancel,
    TaskClaim,
    TaskComplete,
    TaskHeartbeat,
    TaskPause,
    TaskResume,
    TaskSubmit,
    WindowClose,
    WindowConfigure,
)
from app.payload.service import PayloadService

router = APIRouter(prefix="/api/payload", tags=["卫星计算载荷热窗口调度"])


def service() -> PayloadService:
    return PayloadService()


@router.post("/windows", status_code=201)
def configure_window(payload: WindowConfigure, actor: str = Query(default="duty-engineer", min_length=1, max_length=120)):
    return service().configure_window(payload.model_dump(), actor)


@router.get("/windows")
def list_windows(status: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_windows(status=status, limit=limit)}


@router.post("/windows/sweep")
def sweep_expired(actor: str = Query(default="payload-sweeper", min_length=1, max_length=120)):
    return service().sweep_expired(actor)


@router.get("/windows/{window_id}")
def get_window(window_id: int):
    return service().get_window(window_id)


@router.post("/windows/{window_id}/close")
def close_window(window_id: int, payload: WindowClose):
    return service().close_window(window_id, payload.actor, payload.reason)


@router.get("/windows/{window_id}/energy")
def window_energy(window_id: int):
    return service().window_energy(window_id)


@router.post("/tasks", status_code=202)
def submit_task(payload: TaskSubmit):
    return service().submit_task(payload.model_dump())


@router.get("/tasks")
def list_tasks(status: str | None = None, requested_by: str | None = None, window_id: int | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_tasks(status=status, requested_by=requested_by, window_id=window_id, limit=limit)}


@router.post("/tasks/claim")
def claim_task(payload: TaskClaim):
    return service().claim(payload.worker_id, payload.lease_seconds)


@router.get("/tasks/{task_id}")
def get_task(task_id: int):
    return service().get_task(task_id)


@router.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: int, payload: TaskHeartbeat):
    return service().heartbeat(task_id, payload.worker_id, payload.progress_seconds, payload.lease_seconds)


@router.post("/tasks/{task_id}/pause")
def pause_task(task_id: int, payload: TaskPause):
    return service().pause(task_id, payload.worker_id, payload.end_reason, payload.progress_seconds)


@router.post("/tasks/{task_id}/resume")
def resume_task(task_id: int, payload: TaskResume):
    return service().resume(task_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskComplete):
    return service().complete(task_id, payload.worker_id, payload.result, payload.metrics, payload.progress_seconds)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: int, payload: TaskCancel):
    return service().cancel(task_id, payload.actor, payload.reason)


@router.get("/tasks/{task_id}/receipt")
def get_receipt(task_id: int):
    return service().get_receipt(task_id)


@router.get("/audit")
def list_audit_events(
    task_id: int | None = None,
    window_id: int | None = None,
    action: str | None = None,
    actor: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
):
    return {"items": service().audit_events(task_id=task_id, window_id=window_id, action=action, actor=actor, since=since, until=until, limit=limit)}


@router.get("/summary")
def summary():
    return service().summary()


@router.post("/plan")
def plan(payload: PlanRequest):
    return service().plan(payload.model_dump())
