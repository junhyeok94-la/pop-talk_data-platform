"""Composition root: register shared plumbing and independent feature routers."""

from airflow_workbench.shared.web import base_app
from airflow_workbench.shared.assets import router as assets_router
from airflow_workbench.dashboard.api import router as dashboard_router
from airflow_workbench.dashboard.work import router as work_router
from airflow_workbench.dashboard.actions import router as actions_router
from airflow_workbench.monitoring.api import router as monitoring_router
from airflow_workbench.model_lab.api import router as lab_router
from airflow_workbench.model_lab.operations_api import router as operations_router
from airflow_workbench.model_lab.legacy_api import router as legacy_router
from airflow_workbench.model_lab.pages import router as pages_router
from airflow_workbench.model_lab.executor_state import app as executor_state_app

# Backward-compatible identity imports for administration tools and test fixtures.
from airflow_workbench.shared.auth import access, get_user
from airflow_workbench.dashboard.api import dashboard_owner
from airflow_workbench.dashboard import protection

app = base_app()

app.add_middleware(protection.Middleware)
app.add_exception_handler(protection.Deferred, protection.deferred_handler)
app.mount("/executor-state", executor_state_app)
for router in (
    assets_router,
    dashboard_router,
    work_router,
    actions_router,
    monitoring_router,
    pages_router,
    legacy_router,
    operations_router,
    lab_router,
):
    app.include_router(router)
