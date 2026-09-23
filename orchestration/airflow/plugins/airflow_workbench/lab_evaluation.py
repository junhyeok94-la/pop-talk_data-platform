"""Compatibility import for existing DAGs and executor images; use model_lab.evaluation."""

import sys
from importlib import import_module

_module = import_module("airflow_workbench.model_lab.evaluation")
if __name__ == "__main__" and hasattr(_module, "main"):
    _module.main()
else:
    sys.modules[__name__] = _module
