"""Distribution integrity, credential preservation, and non-mutating check rules."""

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if not (SCRIPTS / "verify_source.py").is_file():
    SCRIPTS = Path("/opt/workbench/scripts")


def module(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


source = module("verify_source")


class SourceTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "app.py").write_text("pass\n", encoding="utf-8")
        self.files = {"app.py": hashlib.sha256((self.root / "app.py").read_bytes()).hexdigest()}
        self.manifest()

    def manifest(self):
        (self.root / "SOURCE_MANIFEST.json").write_text(json.dumps({"format": 1, "files": self.files}), encoding="utf-8")

    def test_valid_source_allows_local_installation_files(self):
        (self.root / ".env").write_text("local credentials are not manifest source", encoding="utf-8")
        self.assertEqual(source.verify(self.root), 1)

    def test_modified_file_is_reported_without_file_contents(self):
        (self.root / "app.py").write_text("private-value", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "^Modified source file: app.py$"):
            source.verify(self.root)

    def test_missing_file_is_reported(self):
        (self.root / "app.py").unlink()
        with self.assertRaisesRegex(ValueError, "Missing source file"):
            source.verify(self.root)

    def test_unsafe_manifest_paths_are_rejected(self):
        for name in ("../outside", "/outside", "C:/outside", "app.py:stream", "a\\b", "a/../b", "./app.py", "a//b", ""):
            with self.subTest(name=name):
                self.files = {name: "0" * 64}
                self.manifest()
                with self.assertRaisesRegex(ValueError, "Unsafe manifest path"):
                    source.verify(self.root)

    def test_empty_or_invalid_manifest_is_rejected(self):
        for value in ({"format": 2, "files": self.files}, {"format": 1, "files": {}}, {"format": 1, "files": {"app.py": "bad"}}):
            with self.subTest(value=value):
                (self.root / "SOURCE_MANIFEST.json").write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaises(ValueError):
                    source.verify(self.root)


class InstallationRulesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.installation = module("verify_installation")

    def test_existing_allows_active_dags_and_unrelated_dags(self):
        self.installation.check_dags([{"dag_id": "evaluation", "is_paused": False}, {"dag_id": "business", "is_paused": False}], {"evaluation"}, existing=True)

    def test_fresh_requires_paused_exact_dag_set(self):
        self.installation.check_dags([{"dag_id": "evaluation", "is_paused": True}], {"evaluation"})
        for dags in ([{"dag_id": "evaluation", "is_paused": False}], [{"dag_id": "evaluation", "is_paused": True}, {"dag_id": "business", "is_paused": True}]):
            with self.subTest(dags=dags), self.assertRaises(AssertionError):
                self.installation.check_dags(dags, {"evaluation"})

    def test_existing_rejects_missing_or_broken_managed_dags(self):
        for dags in ([], [{"dag_id": "evaluation", "is_stale": True}], [{"dag_id": "evaluation", "has_import_errors": True}]):
            with self.subTest(dags=dags), self.assertRaises(AssertionError):
                self.installation.check_dags(dags, {"evaluation"}, existing=True)


class CredentialsTest(unittest.TestCase):
    def test_configure_generates_credentials_and_refuses_replacement(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "scripts").mkdir()
            script = root / "scripts" / "configure.py"
            shutil.copyfile(SCRIPTS / "configure.py", script)
            result = subprocess.run([sys.executable, str(script), "--port", "18080"], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            before = (root / ".env").read_bytes()
            values = dict(line.split("=", 1) for line in before.decode().splitlines() if line and not line.startswith("#"))
            password = values["AIRFLOW_ADMIN_PASSWORD"].strip("'")
            self.assertGreaterEqual(len(password), 16)
            self.assertNotIn(password, result.stdout + result.stderr)
            connection = json.loads(values["AIRFLOW_CONN_MODEL_LAB_WORKER"].strip("'"))
            self.assertEqual(connection["password"], values["MODEL_WORKER_TOKEN"].strip("'"))
            result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual((root / ".env").read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
