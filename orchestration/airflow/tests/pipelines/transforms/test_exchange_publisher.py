import hashlib
import io
import json
import unittest
import zipfile

from pipelines.transforms.databricks_bridge import BridgeError
from pipelines.transforms.exchange_publisher import _put_s3_immutable, publish_exchange_bundle
from pipelines.transforms.movie_bronze_silver import digest, packed


class Body:
    def __init__(self, value): self.value = value
    def read(self): return self.value


class Missing(Exception):
    response = {"ResponseMetadata": {"HTTPStatusCode": 404}}


class Precondition(Exception):
    response = {"ResponseMetadata": {"HTTPStatusCode": 412}}


class S3:
    def __init__(self): self.objects = {}
    def get_object(self, Bucket, Key):
        if Key not in self.objects: raise Missing()
        return {"Body": Body(self.objects[Key])}
    def put_object(self, Bucket, Key, Body, **kwargs):
        if Key in self.objects: raise AssertionError("overwrite attempted")
        self.objects[Key] = Body


def archive(movie_title="A"):
    movies = packed({"canonical_movie_key": "k1", "title_ko": movie_title}) + b"\n"
    boxoffice = packed({"target_date": "2026-09-08", "kofic_movie_cd": "1"}) + b"\n"
    documents = {"silver_movies.jsonl": movies, "silver_boxoffice.jsonl": boxoffice}
    index = {"bundle_version": 1, "run_id": "run1", "ready_key": "manifests/ready.json",
             "artifact_version": "a" * 64,
             "documents": {name: digest(body) for name, body in documents.items()},
             "expected": {"silver_movies": 1, "silver_boxoffice": 1}}
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as bundle:
        for name, body in documents.items(): bundle.writestr(name, body)
        bundle.writestr("bundle_index.json", packed(index))
    return output.getvalue()


def publish(s3, body):
    return publish_exchange_bundle(
        s3, bucket="b", archive=body,
        expected_archive_sha256=hashlib.sha256(body).hexdigest(),
        expected_run_id="run1", expected_artifact_version="a" * 64,
        publication_revision=2,
    )


class ExchangePublisherTests(unittest.TestCase):
    def test_ready_is_written_last_and_replay_is_idempotent(self):
        s3 = S3()
        first = publish(s3, archive())
        self.assertEqual(1, first.movie_count)
        self.assertTrue(list(s3.objects)[-1].endswith("EXCHANGE_READY.json"))
        self.assertEqual(first, publish(s3, archive()))

    def test_same_identity_with_different_content_fails_closed(self):
        s3 = S3()
        publish(s3, archive("A"))
        with self.assertRaisesRegex(BridgeError, "conflict"):
            publish(s3, archive("B"))

    def test_missing_required_checksum_is_rejected_before_s3_write(self):
        body = archive()
        source = zipfile.ZipFile(io.BytesIO(body))
        index = json.loads(source.read("bundle_index.json"))
        index["documents"].pop("silver_movies.jsonl")
        changed = io.BytesIO()
        with zipfile.ZipFile(changed, "w") as target:
            for name in source.namelist():
                target.writestr(name, packed(index) if name == "bundle_index.json" else source.read(name))
        with self.assertRaisesRegex(BridgeError, "required document checksums"):
            publish(S3(), changed.getvalue())

    def test_partial_data_failure_leaves_no_ready_and_retry_recovers(self):
        class FailSecondFile(S3):
            fail = True
            def put_object(self, Bucket, Key, Body, **kwargs):
                if self.fail and Key.endswith("boxoffice.jsonl"):
                    raise RuntimeError("injected write failure")
                super().put_object(Bucket, Key, Body, **kwargs)

        s3 = FailSecondFile()
        with self.assertRaisesRegex(RuntimeError, "injected"):
            publish(s3, archive())
        self.assertFalse(any(key.endswith("EXCHANGE_READY.json") for key in s3.objects))
        s3.fail = False
        result = publish(s3, archive())
        self.assertIn(result.ready_key, s3.objects)

    def test_concurrent_create_accepts_only_identical_bytes(self):
        class Race(S3):
            def __init__(self, winner):
                super().__init__()
                self.winner = winner
            def put_object(self, Bucket, Key, Body, **kwargs):
                self.objects[Key] = self.winner if self.winner is not None else Body
                raise Precondition()

        same = Race(None)
        _put_s3_immutable(same, "b", "key", b"same")
        different = Race(b"different")
        with self.assertRaisesRegex(BridgeError, "Concurrent immutable"):
            _put_s3_immutable(different, "b", "key", b"same")


if __name__ == "__main__":
    unittest.main()
