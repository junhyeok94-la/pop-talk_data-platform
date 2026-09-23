"""Verify release naming, reproducibility, and the deployment-template allowlist."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import export_airflow_workbench as exporter


class ExportTest(unittest.TestCase):
    def test_dotted_release_name_and_reproducible_archive(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "app.py"
            source.write_text("pass\n", encoding="utf-8")
            archives = []
            for parent in ("first", "second"):
                destination = root / parent / "workbench-0.1.2-dev"
                with patch.object(exporter, "sources", return_value=[(source, Path("app.py"))]), contextlib.redirect_stdout(io.StringIO()):
                    exporter.export(destination, archive=True)
                archive = destination.parent / (destination.name + ".zip")
                archives.append(archive.read_bytes())
                self.assertEqual((destination.parent / (destination.name + ".zip.sha256")).read_text().split()[0], hashlib.sha256(archive.read_bytes()).hexdigest())
                with zipfile.ZipFile(archive) as bundle:
                    self.assertIsNone(bundle.testzip())
                    manifest = json.loads(bundle.read(destination.name + "/SOURCE_MANIFEST.json"))["files"]
                    self.assertEqual(manifest["app.py"], hashlib.sha256(bundle.read(destination.name + "/app.py")).hexdigest())
            self.assertEqual(*archives)

    def test_existing_archive_is_not_overwritten_or_partially_exported(self):
        with tempfile.TemporaryDirectory() as folder:
            destination = Path(folder) / "workbench-0.1.2-dev"
            archive = destination.parent / (destination.name + ".zip")
            archive.write_bytes(b"existing release")
            with self.assertRaises(SystemExit):
                exporter.export(destination, archive=True)
            self.assertFalse(destination.exists())
            self.assertEqual(archive.read_bytes(), b"existing release")

    def test_unlisted_template_files_never_enter_export(self):
        with tempfile.TemporaryDirectory() as folder:
            template = Path(folder)
            for name in (".env", "credentials.json", "database.sql", "private-key.pem"):
                (template / name).write_text("private", encoding="utf-8")
            with patch.object(exporter, "TEMPLATE", template):
                selected = {relative.as_posix() for _, relative in exporter.sources()}
            self.assertTrue(set(exporter.TEMPLATE_FILES) <= selected)
            self.assertFalse(selected & {".env", "credentials.json", "database.sql", "private-key.pem"})


if __name__ == "__main__":
    unittest.main()
