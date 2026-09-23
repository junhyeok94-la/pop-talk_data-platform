"""Dashboard storage always uses the bounded pool, including non-HTTP callers."""

from airflow_workbench.shared.database import Store as BaseStore, Conflict, NotFound
from airflow_workbench.dashboard import protection


class Store(BaseStore):
    def connection(self):
        return protection.store_connection(self.schema)
