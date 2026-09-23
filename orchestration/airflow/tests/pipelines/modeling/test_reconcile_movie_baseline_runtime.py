import hashlib
import json
import unittest

from scripts.reconcile_movie_baseline import ReconciliationRuntimeError, _load_legacy_from_client


class _Body:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


class _Client:
    def __init__(self, manifest, source):
        self.manifest = manifest
        self.source = source
        self.keys = []

    def get_object(self, *, Bucket, Key):
        self.keys.append(Key)
        value = self.manifest if len(self.keys) == 1 else self.source
        return {"Body": _Body(value)}


class ReconciliationRuntimeTest(unittest.TestCase):
    def _manifest(self, source):
        return {
            "schema_version": 1, "status": "SUCCESS", "bucket": "bucket",
            "key": "raw/legacy_snapshot/v1/sha256=x/movies.json",
            "bytes": len(source), "sha256": hashlib.sha256(source).hexdigest(),
            "record_count": 1,
        }

    def test_invalid_manifest_does_not_request_source_object(self):
        source = json.dumps([{"movie_cd": "001"}]).encode()
        for mutation in (
            {"status": "FAILED"}, {"bucket": "other"}, {"key": "raw/other/data.json"},
        ):
            manifest = {**self._manifest(source), **mutation}
            client = _Client(json.dumps(manifest).encode(), source)
            with self.assertRaises(ReconciliationRuntimeError):
                _load_legacy_from_client(client, "bucket", "manifest.json")
            self.assertEqual(client.keys, ["manifest.json"])

    def test_failure_message_has_only_stage_and_allowlisted_code(self):
        error = ReconciliationRuntimeError("s3_source", "SHA256_MISMATCH")
        self.assertEqual(error.stage, "s3_source")
        self.assertEqual(error.code, "SHA256_MISMATCH")
        self.assertNotIn("password", str(error).lower())


if __name__ == "__main__":
    unittest.main()
