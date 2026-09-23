"""Compatibility import for existing DAGs and executor images; use dashboard.query."""

import sys
from importlib import import_module

_module = import_module("airflow_workbench.dashboard.query")
if __name__ == "__main__" and hasattr(_module, "main"):
    _module.main()
else:
    sys.modules[__name__] = _module
