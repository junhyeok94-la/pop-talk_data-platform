"""Compatibility facade for earlier administration scripts."""

from airflow_workbench.shared import metadata
from airflow_workbench.shared.database import Conflict, NotFound
from airflow_workbench.dashboard.legacy_store import (
    Store as DashboardStore,
    DEFAULT_DASHBOARD,
)
from airflow_workbench.model_lab.legacy_store import Store as ModelStore


class Store(DashboardStore, ModelStore):
    pass
