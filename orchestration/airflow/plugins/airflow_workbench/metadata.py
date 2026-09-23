"""Compatibility import for existing DAGs and executor images; use shared.metadata."""

import sys
from importlib import import_module

_module = import_module("airflow_workbench.shared.metadata")
if __name__ == "__main__":
    _module.migrate()
    print("Workbench PostgreSQL schema ready")
else:
    sys.modules[__name__] = _module
