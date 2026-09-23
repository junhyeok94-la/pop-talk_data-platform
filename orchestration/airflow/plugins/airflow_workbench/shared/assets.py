"""Explicit asset allowlist; each document loads only its own feature bundles."""

from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter()
ROOT = Path(__file__).resolve().parents[1]
ASSETS = {
    "dashboard/workspace.js": "dashboard/static/workspace.js",
    "shared/host.js": "shared/static/host.js",
    "shared/navigation.js": "shared/static/navigation.js",
    "dashboard/work.js": "dashboard/static/work.js",
    "dashboard/work.css": "dashboard/static/work.css",
    "monitoring/monitoring.js": "monitoring/static/monitoring.js",
    "monitoring/monitoring.css": "monitoring/static/monitoring.css",
    "monitoring/monitoring.svg": "monitoring/static/monitoring.svg",
    "shared/ui.js": "shared/static/ui.js",
    "model_lab/runtime.js": "model_lab/static/runtime.js",
    "model_lab/bootstrap.js": "model_lab/static/bootstrap.js",
    "dashboard/runtime.js": "dashboard/static/runtime.js",
    "dashboard/bootstrap.js": "dashboard/static/bootstrap.js",
    "shared/theme.js": "shared/static/theme.js",
    "shared/theme.css": "shared/static/theme.css",
    "dashboard/studio.js": "dashboard/static/studio.js",
    "dashboard/studio.css": "dashboard/static/studio.css",
    "dashboard/studio-vendor.js": "dashboard/static/studio-vendor.js",
    "dashboard/studio-vendor.css": "dashboard/static/studio-vendor.css",
    "dashboard/studio-vendor.js.LEGAL.txt": "dashboard/static/studio-vendor.js.LEGAL.txt",
    "dashboard/THIRD-PARTY-NOTICES.txt": "dashboard/static/THIRD-PARTY-NOTICES.txt",
    "dashboard/dashboard.svg": "dashboard/static/dashboard.svg",
    "model_lab/lab.js": "model_lab/static/lab.js",
    "model_lab/lab.css": "model_lab/static/lab.css",
    "model_lab/mlops.js": "model_lab/static/mlops.js",
    "model_lab/models.svg": "model_lab/static/models.svg",
    "shared/ui.css": "shared/static/ui.css",
    "model_lab/runtime.css": "model_lab/static/runtime.css",
    "dashboard/legacy.css": "dashboard/static/legacy.css",
}
LEGACY = {
    "theme.js": "shared/static/theme.js",
    "theme.css": "shared/static/theme.css",
    "studio.js": "dashboard/static/studio.js",
    "studio.css": "dashboard/static/studio.css",
    "studio-vendor.js": "dashboard/static/studio-vendor.js",
    "studio-vendor.css": "dashboard/static/studio-vendor.css",
    "studio-vendor.js.LEGAL.txt": "dashboard/static/studio-vendor.js.LEGAL.txt",
    "THIRD-PARTY-NOTICES.txt": "dashboard/static/THIRD-PARTY-NOTICES.txt",
    "dashboard.svg": "dashboard/static/dashboard.svg",
    "lab.js": "model_lab/static/lab.js",
    "lab.css": "model_lab/static/lab.css",
    "mlops.js": "model_lab/static/mlops.js",
    "models.svg": "model_lab/static/models.svg",
    "workbench.css": "shared/static/ui.css",
    "dashboard.css": "model_lab/static/runtime.css",
}


@router.get("/static/{feature}/{filename}")
async def asset(feature: str, filename: str):
    target = ASSETS.get(feature + "/" + filename)
    if target is None:
        raise HTTPException(404)
    return FileResponse(ROOT / target)


@router.get("/static/{filename}")
async def legacy_asset(filename: str):
    target = LEGACY.get(filename)
    if target is None:
        raise HTTPException(404)
    return FileResponse(ROOT / target)
