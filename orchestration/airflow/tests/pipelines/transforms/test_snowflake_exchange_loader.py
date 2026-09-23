import hashlib
import json
import unittest

from pipelines.transforms.movie_bronze_silver import packed
from pipelines.transforms.snowflake_exchange_loader import SnowflakeLoadError, read_exchange


class Body:
    def __init__(self, value): self.value = value
    def read(self): return self.value


class S3:
    def __init__(self, objects): self.objects = objects
    def get_object(self, Bucket, Key): return {"Body": Body(self.objects[Key])}


def fixture():
    artifact = "a" * 64
    common = {"source_run_id": "run1", "transform_version": artifact,
              "ready_manifest_key": "raw-ready", "source_observed_at": "2026-09-09T00:00:00Z"}
    files = {"movies.jsonl": packed({**common, "canonical_movie_key": "m1"}) + b"\n",
             "boxoffice.jsonl": packed({**common, "target_date": "2026-09-08",
                                          "kofic_movie_cd": "1"}) + b"\n"}
    ready = {"schema_version": 1, "exchange_contract_version": 1,
             "source_bundle_sha256": "b" * 64, "source_run_id": "run1",
             "source_ready_key": "raw-ready", "artifact_version": artifact,
             "publication_revision": 4,
             "files": {name: {"key": name, "sha256": hashlib.sha256(body).hexdigest(),
                                "row_count": 1} for name, body in files.items()}}
    ready_key = ("exchange/movie_silver/v1/source_run_id=run1/artifact_version="
                 + artifact + "/publication_revision=4/EXCHANGE_READY.json")
    return {**files, ready_key: packed(ready)}, ready_key


class LoaderInputTests(unittest.TestCase):
    def test_reads_complete_observation_contract(self):
        objects, ready_key = fixture()
        result = read_exchange(S3(objects), bucket="b", ready_key=ready_key)
        self.assertEqual(("run1", 4, 1, 1),
                         (result.source_run_id, result.publication_revision,
                          len(result.movies), len(result.boxoffice)))

    def test_checksum_and_row_identity_fail_closed(self):
        objects, ready_key = fixture()
        objects["movies.jsonl"] += b" "
        with self.assertRaisesRegex(SnowflakeLoadError, "checksum"):
            read_exchange(S3(objects), bucket="b", ready_key=ready_key)
        objects, ready_key = fixture()
        row = json.loads(objects["movies.jsonl"])
        row["source_run_id"] = "other"
        objects["movies.jsonl"] = packed(row) + b"\n"
        ready = json.loads(objects[ready_key])
        ready["files"]["movies.jsonl"]["sha256"] = hashlib.sha256(objects["movies.jsonl"]).hexdigest()
        objects[ready_key] = packed(ready)
        with self.assertRaisesRegex(SnowflakeLoadError, "identity"):
            read_exchange(S3(objects), bucket="b", ready_key=ready_key)


if __name__ == "__main__": unittest.main()
