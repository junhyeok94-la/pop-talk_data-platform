"""Metadata connection and common errors, with no feature-specific operations."""

import os
from airflow_workbench.shared import metadata


class Conflict(Exception):
    pass


class NotFound(Exception):
    pass


class Store:
    def __init__(self):
        self.schema = metadata.schema_name(
            os.environ.get("AIRFLOW_WORKBENCH_DB_SCHEMA", "workbench")
        )

    def connection(self):
        return metadata.connection(self.schema)
