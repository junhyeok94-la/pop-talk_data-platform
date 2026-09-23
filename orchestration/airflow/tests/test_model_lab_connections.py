"""Resolve only connection metadata, honoring backend precedence without secrets."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.secrets.metastore import MetastoreBackend
from airflow_workbench.model_lab import connections


class ConnectionTest(unittest.TestCase):
    def test_environment_connection_is_discoverable_without_database_entry(self):
        value = json.dumps({"conn_type": "http", "host": "http://private.example", "password": "private-token"})
        backend = EnvironmentVariablesBackend()
        with patch.dict("os.environ", {"AIRFLOW_CONN_TEST_INFERENCE": value}), patch("airflow.configuration.ensure_secrets_loaded", return_value=[backend]):
            result = connections.describe("test_inference")
            self.assertIn("test_inference", connections.environment_ids())
        self.assertEqual(result["source"], "environment")
        self.assertTrue(result["valid"])
        self.assertNotIn("private", json.dumps(result))

    def test_external_backend_overrides_environment_source(self):
        external = Mock()
        external.get_connection.return_value = SimpleNamespace(host="https://external.example", password="secret")
        env = EnvironmentVariablesBackend()
        with patch("airflow.configuration.ensure_secrets_loaded", return_value=[external, env]), patch.object(env, "get_connection") as fallback:
            result = connections.describe("shared")
        self.assertEqual(result["source"], "secrets_backend")
        fallback.assert_not_called()

    def test_metadata_and_missing_or_unavailable_are_distinguished(self):
        backend = MetastoreBackend()
        with patch("airflow.configuration.ensure_secrets_loaded", return_value=[backend]), patch.object(backend, "get_connection", return_value=SimpleNamespace(host="postgresql://db")):
            result = connections.describe("database")
        self.assertEqual(result["source"], "metadata")
        self.assertFalse(result["valid"])
        with patch("airflow.configuration.ensure_secrets_loaded", return_value=[backend]), patch.object(backend, "get_connection", return_value=None):
            self.assertEqual(connections.describe("absent")["source"], "missing")
        with patch("airflow.configuration.ensure_secrets_loaded", return_value=[backend]), patch.object(backend, "get_connection", side_effect=RuntimeError("private-token")):
            result = connections.describe("unavailable")
        self.assertEqual(result["source"], "unavailable")
        self.assertNotIn("private-token", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
