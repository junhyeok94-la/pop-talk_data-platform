import hashlib
import json
import unittest

from pipelines.transforms.legacy_candidate_bridge import (
    LegacyCandidateError,
    build_legacy_candidate_archive,
    stage_legacy_candidate_bundle,
)
from pipelines.transforms.legacy_candidate_exchange import (
    EMPTY_SHA256,
    publish_legacy_candidate_exchange,
)
from pipelines.transforms.legacy_candidate_snowflake import (
    LegacyCandidateInput,
    LegacyCandidateLoadError,
    load_legacy_candidate,
    read_legacy_candidate_exchange,
)
from pipelines.transforms.movie_bronze_silver import packed


class Body:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


class Missing(Exception):
    response = {"ResponseMetadata": {"HTTPStatusCode": 404}}


class S3:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.gets = []
        self.puts = []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise Missing()
        return {"Body": Body(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if Key in self.objects:
            raise AssertionError("overwrite attempted")
        self.objects[Key] = Body
        self.puts.append(Key)


class Files:
    def __init__(self):
        self.values = {}

    def mkdir(self, path):
        self.directory = path

    def put_immutable(self, path, body):
        previous = self.values.get(path)
        if previous is not None and previous != body:
            raise LegacyCandidateError("immutable conflict")
        self.values[path] = body
        return hashlib.sha256(body).hexdigest()


def source_fixture():
    source = packed([
        {"movie_cd": "2", "movie_nm": "둘", "open_dt": "20190101"},
        {"movie_cd": "1", "movie_nm": "하나", "open_dt": "20260101"},
    ])
    source_sha = hashlib.sha256(source).hexdigest()
    source_key = f"raw/legacy_snapshot/v1/sha256={source_sha}/movies_final.json"
    manifest_key = f"manifests/legacy_snapshot/v1/sha256={source_sha}/SUCCESS.json"
    manifest = packed({
        "schema_version": 1,
        "status": "SUCCESS",
        "bucket": "bucket",
        "key": source_key,
        "bytes": len(source),
        "sha256": source_sha,
        "record_count": 2,
    })
    return source, source_sha, source_key, manifest_key, manifest


def archive_fixture():
    source, source_sha, _, manifest_key, manifest = source_fixture()
    archive, index = build_legacy_candidate_archive(
        bucket="bucket",
        manifest_key=manifest_key,
        manifest_body=manifest,
        source_body=source,
        transform_source=b"# transform",
        artifact_version="a" * 64,
    )
    return archive, index


class LegacyBundleTests(unittest.TestCase):
    def test_stage_rejects_manifest_path_before_any_s3_get(self):
        s3 = S3({"raw/outside-candidate/SUCCESS.json": b"{}"})
        files = Files()
        with self.assertRaisesRegex(LegacyCandidateError, "allowlist"):
            stage_legacy_candidate_bundle(
                s3, files, bucket="bucket",
                manifest_key="raw/outside-candidate/SUCCESS.json",
                transform_source=b"# transform", artifact_version="a" * 64,
            )
        self.assertEqual(s3.gets, [])
        self.assertEqual(files.values, {})

    def test_bundle_is_deterministic_and_counts_policy(self):
        first, index = archive_fixture()
        second, _ = archive_fixture()
        self.assertEqual(first, second)
        self.assertEqual(index["expected"], {
            "bronze_objects": 1,
            "silver_movies": 2,
            "silver_boxoffice": 0,
            "eligible_movies": 1,
            "excluded_movies": 1,
        })

    def test_manifest_path_sha_must_match_body(self):
        source, _, _, manifest_key, manifest = source_fixture()
        wrong = manifest_key.replace("sha256=", "sha256=" + "0" * 64 + "/x=")
        with self.assertRaisesRegex(LegacyCandidateError, "allowlist"):
            build_legacy_candidate_archive(
                bucket="bucket", manifest_key=wrong, manifest_body=manifest,
                source_body=source, transform_source=b"x", artifact_version="a" * 64,
            )

    def test_stage_writes_one_immutable_archive(self):
        source, _, source_key, manifest_key, manifest = source_fixture()
        s3 = S3({manifest_key: manifest, source_key: source})
        files = Files()
        result = stage_legacy_candidate_bundle(
            s3, files, bucket="bucket", manifest_key=manifest_key,
            transform_source=b"# transform", artifact_version="a" * 64,
        )
        self.assertEqual((result.movie_count, result.eligible_count, result.excluded_count),
                         (2, 1, 1))
        self.assertEqual(list(files.values), [result.archive_path])

    def test_stage_rejects_source_path_before_get(self):
        source, source_sha, _, manifest_key, _ = source_fixture()
        malicious_manifest = packed({
            "schema_version": 1, "status": "SUCCESS", "bucket": "bucket",
            "key": "raw/other-secret.json", "bytes": len(source),
            "sha256": source_sha, "record_count": 2,
        })
        s3 = S3({manifest_key: malicious_manifest, "raw/other-secret.json": source})
        with self.assertRaisesRegex(LegacyCandidateError, "manifest contract|path identity"):
            stage_legacy_candidate_bundle(
                s3, Files(), bucket="bucket", manifest_key=manifest_key,
                transform_source=b"# transform", artifact_version="a" * 64,
            )
        self.assertEqual(s3.gets, [manifest_key])


class LegacyExchangeTests(unittest.TestCase):
    def publish(self, s3):
        archive, index = archive_fixture()
        return publish_legacy_candidate_exchange(
            s3,
            bucket="bucket",
            archive=archive,
            expected_archive_sha256=hashlib.sha256(archive).hexdigest(),
            expected_source_sha256=index["source_sha256"],
            expected_artifact_version=index["artifact_version"],
            publication_revision=1,
        )

    def test_data_first_ready_last_and_empty_boxoffice(self):
        s3 = S3()
        result = self.publish(s3)
        self.assertTrue(s3.puts[-1].endswith("EXCHANGE_READY.json"))
        self.assertEqual(s3.objects[result.prefix + "/boxoffice.jsonl"], b"")
        ready = json.loads(s3.objects[result.ready_key])
        self.assertEqual(ready["files"]["boxoffice.jsonl"], {
            "bytes": 0,
            "key": result.prefix + "/boxoffice.jsonl",
            "row_count": 0,
            "sha256": EMPTY_SHA256,
        })
        self.assertEqual(self.publish(s3), result)

    def test_reader_validates_child_paths_before_get(self):
        s3 = S3()
        result = self.publish(s3)
        ready = json.loads(s3.objects[result.ready_key])
        ready["files"]["movies.jsonl"]["key"] = "raw/not-candidate.jsonl"
        s3.objects[result.ready_key] = packed(ready)
        s3.gets.clear()
        with self.assertRaisesRegex(LegacyCandidateLoadError, "child key"):
            read_legacy_candidate_exchange(s3, bucket="bucket", ready_key=result.ready_key)
        self.assertEqual(s3.gets, [result.ready_key])

    def test_reader_accepts_v2_unknown_time_contract(self):
        s3 = S3()
        result = self.publish(s3)
        loaded = read_legacy_candidate_exchange(
            s3, bucket="bucket", ready_key=result.ready_key
        )
        self.assertEqual((len(loaded.movies), loaded.source_run_id),
                         (2, result.source_run_id))


class RecordingCursor:
    def __init__(self, movie_count, *, previous=None, conflict_count=0, missing_count=0):
        self.movie_count = movie_count
        self.previous = previous
        self.conflict_count = conflict_count
        self.missing_count = missing_count
        self.commands = []
        self._result = None

    def execute(self, sql, params=None):
        compact = " ".join(sql.split())
        self.commands.append((compact, params))
        if compact.startswith("SELECT SOURCE_RUN_ID"):
            self._result = self.previous
        elif (
            compact.startswith(
                "SELECT COUNT(*) FROM POP_TALK_DW_DEV.CANDIDATE.MOVIE_OBSERVATIONS_RAW"
            )
            and " JOIN " not in compact
        ):
            self._result = (self.movie_count,)
        elif compact.startswith("SELECT COUNT(*)"):
            self._result = (
                self.missing_count if "LEFT JOIN" in compact else self.conflict_count,
            )

    def executemany(self, sql, values):
        self.commands.append((" ".join(sql.split()), tuple(values)))

    def fetchone(self):
        return self._result

    def close(self):
        self.closed = True


class RecordingConnection:
    def __init__(self, movie_count, **cursor_options):
        self.cursor_value = RecordingCursor(movie_count, **cursor_options)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def load_fixture():
    source_sha = "b" * 64
    artifact = "a" * 64
    row = {
        "canonical_movie_key": "kofic:1",
        "source_run_id": f"legacy:{source_sha}",
        "transform_version": artifact,
        "ready_manifest_key": (
            f"manifests/legacy_snapshot/v1/sha256={source_sha}/SUCCESS.json"
        ),
        "source_observed_at": None,
        "source_observed_at_known": False,
        "source_time_basis": "LEGACY_UNKNOWN",
    }
    return LegacyCandidateInput(
        ready_key=(
            "exchange/candidate/movie_legacy/v2/"
            f"source_sha256={source_sha}/artifact_version={artifact}/"
            "publication_revision=1/EXCHANGE_READY.json"
        ),
        source_run_id=f"legacy:{source_sha}",
        source_sha256=source_sha, artifact_version=artifact,
        publication_revision=1, bundle_sha256="c" * 64,
        manifest_key=f"manifests/legacy_snapshot/v1/sha256={source_sha}/SUCCESS.json",
        movies=(row,),
    )


class LegacySnowflakeTransactionTests(unittest.TestCase):
    def test_direct_loader_rejects_invalid_time_contract_before_ddl(self):
        valid = load_fixture()
        invalid_row = {**valid.movies[0], "source_time_basis": "API_COLLECTED_AT"}
        invalid = LegacyCandidateInput(
            **{**valid.__dict__, "movies": (invalid_row,)}
        )
        connection = RecordingConnection(1)
        with self.assertRaisesRegex(LegacyCandidateLoadError, "lineage/time"):
            load_legacy_candidate(connection, invalid)
        self.assertEqual(connection.cursor_value.commands, [])

    def test_ddl_and_temp_input_are_prepared_before_begin(self):
        connection = RecordingConnection(1)
        self.assertEqual(load_legacy_candidate(connection, load_fixture()), 1)
        statements = [sql for sql, _ in connection.cursor_value.commands]
        begin = statements.index("BEGIN")
        self.assertFalse(any(sql.startswith("CREATE") for sql in statements[begin + 1:]))
        self.assertEqual((connection.commits, connection.rollbacks), (1, 0))

    def test_failure_after_merge_rolls_back_without_success_update(self):
        connection = RecordingConnection(1)
        with self.assertRaisesRegex(LegacyCandidateLoadError, "Injected"):
            load_legacy_candidate(
                connection, load_fixture(), fail_after_movie_merge=True
            )
        statements = [sql for sql, _ in connection.cursor_value.commands]
        self.assertFalse(any(sql.startswith("UPDATE") for sql in statements))
        self.assertEqual((connection.commits, connection.rollbacks), (0, 1))

    def test_successful_replay_revalidates_persisted_rows_without_merge(self):
        exchange = load_fixture()
        previous = (
            exchange.source_run_id, exchange.source_sha256, exchange.artifact_version,
            exchange.publication_revision, exchange.bundle_sha256, "SUCCESS", 1,
        )
        connection = RecordingConnection(1, previous=previous)
        self.assertEqual(load_legacy_candidate(connection, exchange), 1)
        statements = [sql for sql, _ in connection.cursor_value.commands]
        self.assertFalse(any(sql.startswith("MERGE INTO") for sql in statements))
        self.assertEqual((connection.commits, connection.rollbacks), (1, 0))

    def test_successful_replay_rejects_persisted_count_or_hash_pollution(self):
        exchange = load_fixture()
        previous = (
            exchange.source_run_id, exchange.source_sha256, exchange.artifact_version,
            exchange.publication_revision, exchange.bundle_sha256, "SUCCESS", 1,
        )
        count_polluted = RecordingConnection(2, previous=previous)
        with self.assertRaisesRegex(LegacyCandidateLoadError, "count mismatch"):
            load_legacy_candidate(count_polluted, exchange)
        hash_polluted = RecordingConnection(1, previous=previous, missing_count=1)
        with self.assertRaisesRegex(LegacyCandidateLoadError, "key/hash mismatch"):
            load_legacy_candidate(hash_polluted, exchange)


if __name__ == "__main__":
    unittest.main()
