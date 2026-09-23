"""Filesystem locations shared by DAG parsing, CLI tools and local tests."""
import os
from pathlib import Path

MODULES_ROOT = Path(os.environ.get("POP_TALK_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
