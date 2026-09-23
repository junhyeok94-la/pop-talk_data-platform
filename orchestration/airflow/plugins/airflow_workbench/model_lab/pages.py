"""Model Lab document and page configuration."""

from pathlib import Path
from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from airflow_workbench.shared.auth import access
from airflow_workbench.shared import auth
from airflow_workbench.model_lab import environments

router = APIRouter()
STATIC = Path(__file__).parent / "static"


@router.get("/models")
async def page():
    return FileResponse(STATIC / "index.html")


@router.get("/api/config")
@router.get("/api/model-lab/config")
async def config(user=Depends(access)):
    manager = auth.get_auth_manager()
    return {
        "project": "Airflow Workbench",
        "can_edit": manager.is_authorized_configuration(method="GET", user=user)
        or manager.is_authorized_custom_view(
            method="PUT", resource_name="Airflow Workbench", user=user
        ),
        "can_edit_dashboard": True,
        "dashboard_scope": "personal",
        "storage_backend": "Airflow PostgreSQL / workbench",
        "environments": [e.model_dump() for e in environments.configured()],
    }
