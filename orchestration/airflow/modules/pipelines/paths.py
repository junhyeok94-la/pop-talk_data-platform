"""Filesystem locations shared by DAG parsing, CLI tools and local tests."""
import os
from pathlib import Path

MODULES_ROOT = Path(os.environ.get("POP_TALK_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
# Compose sets this explicitly; the fallback supports tests from the source checkout.
DBT_DEPLOYMENT_ROOT = Path(os.environ.get(
    "POP_TALK_DBT_DEPLOYMENT_ROOT",
    Path(__file__).resolve().parents[4] / ".local" / "data" / "dbt-deployments",
))
