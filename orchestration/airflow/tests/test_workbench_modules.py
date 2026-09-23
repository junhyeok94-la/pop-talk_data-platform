"""Feature boundaries, public routes/assets and persisted Python import compatibility."""

import importlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import unittest
from html.parser import HTMLParser
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient
from airflow_workbench.app import app, access


class Assets(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "script":
            self.urls.append(values["src"])
        elif tag == "link":
            self.urls.append(values["href"])


class ModuleBoundaryTest(unittest.TestCase):
    def test_each_feature_imports_without_the_other_runtime(self):
        for target, blocked in (("dashboard.api", "model_lab"), ("model_lab.api", "dashboard")):
            source = f"""
import sys, importlib, importlib.abc
class DenyOtherFeature(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('airflow_workbench.{blocked}'):
            raise AssertionError('Cross-feature runtime dependency: ' + fullname)
sys.meta_path.insert(0, DenyOtherFeature())
importlib.import_module('airflow_workbench.{target}')
"""
            result = subprocess.run([sys.executable, "-c", source], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_documents_load_only_their_feature_and_shared_assets(self):
        from workbench_test_db import isolated_database
        isolated_database(self)
        app.dependency_overrides[access] = lambda: SimpleNamespace(get_id=lambda: "test")
        self.addCleanup(app.dependency_overrides.clear)
        with TestClient(app) as client:
            for page, feature in (("dashboard", "dashboard"), ("dashboard?view=charts", "dashboard"), ("models", "model_lab"), ("monitoring", "monitoring")):
                response = client.get("/" + page)
                self.assertEqual(response.status_code, 200)
                parsed = Assets()
                parsed.feed(response.text)
                self.assertTrue(parsed.urls)
                for url in parsed.urls:
                    self.assertIn(url.split("/")[1], (feature, "shared"))
                    asset = client.get("/" + url)
                    self.assertEqual(asset.status_code, 200, url)
                    self.assertIn("frame-ancestors 'self'", asset.headers["content-security-policy"])
                self.assertNotIn("workbench.js", response.text)
            self.assertEqual(client.get("/static/shared/not-allowed.py").status_code, 404)
            self.assertEqual(client.get("/static/other/ui.js").status_code, 404)

    def test_legacy_serialized_dag_imports_keep_class_identity(self):
        pairs = (
            ("lab_dag", "model_lab.evaluation_dag", "EvaluationTrigger"),
            ("mlops", "model_lab.mlops", "ModelJobTrigger"),
            ("kubernetes_executor", "model_lab.kubernetes_executor", "PortableTrainingPodOperator"),
            ("lab_contracts", "model_lab.contracts", "Endpoint"),
            ("schemas", "model_lab.schemas", "TrainingRecipe"),
        )
        for old, new, symbol in pairs:
            first = getattr(importlib.import_module("airflow_workbench." + old), symbol)
            second = getattr(importlib.import_module("airflow_workbench." + new), symbol)
            self.assertIs(first, second)

    def test_metadata_cli_keeps_migration_entrypoint(self):
        # Invoke in a fresh process so runpy does not encounter an imported alias.
        source = """
import runpy
from unittest.mock import patch
with patch('airflow_workbench.shared.metadata.migrate') as migrate:
    runpy.run_module('airflow_workbench.metadata', run_name='__main__')
    migrate.assert_called_once_with()
"""
        result = subprocess.run([sys.executable, "-c", source], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_worker_contracts_do_not_import_airflow_or_dashboard(self):
        source = """
import sys, importlib, importlib.abc
class NoControlPlane(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'airflow' or fullname.startswith(('airflow.', 'airflow_workbench.dashboard', 'airflow_workbench.shared.database', 'airflow_workbench.shared.metadata')):
            raise AssertionError(fullname)
sys.meta_path.insert(0, NoControlPlane())
for name in ('schemas', 'training', 'portable_training', 'storage'):
    importlib.import_module('airflow_workbench.model_lab.' + name)
"""
        result = subprocess.run([sys.executable, "-c", source], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
