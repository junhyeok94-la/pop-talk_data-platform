"""초기 적재 adapter는 원문 시각을 보존하고 검증된 입력만 공통 loader에 전달한다."""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pipelines.orchestration.movie_initial_load import load_initial_snapshot


class InitialLoadTests(unittest.TestCase):
    def setUp(self):
        self.source = SimpleNamespace(ready_key="ready", source_run_id="legacy:sha", source_sha256="sha",
            artifact_version="artifact", publication_revision=1, bundle_sha256="bundle",
            movies=({"canonical_movie_key": "kofic:1", "source_observed_at": None,
                     "source_observed_at_known": False, "source_time_basis": "LEGACY_UNKNOWN"},))
        self.published = {key: getattr(self.source, key) for key in
            ("ready_key", "source_run_id", "source_sha256", "artifact_version", "publication_revision")}
        self.published.update(movie_count=1, boxoffice_count=0)
        self.connection = MagicMock()

    def invoke(self, **kwargs):
        return load_initial_snapshot(s3_client=object(), snowflake_connection=self.connection,
                                     bucket="bucket", published=self.published, **kwargs)

    @patch("pipelines.orchestration.movie_initial_load.load_exchange", return_value=(1, 0))
    @patch("pipelines.orchestration.movie_initial_load.read_legacy_candidate_exchange")
    def test_unknown_time_and_empty_boxoffice_are_preserved(self, read, load):
        read.return_value = self.source
        result = self.invoke()
        exchange = load.call_args.args[1]
        self.assertIsNone(exchange.movies[0]["source_observed_at"])
        self.assertEqual(exchange.boxoffice, ())
        self.assertEqual(result["movie_count"], 1)
        sql = [call.args[0] for call in self.connection.cursor.return_value.__enter__.return_value.execute.call_args_list]
        self.assertTrue(any("DROP NOT NULL" in query for query in sql))
        self.assertFalse(any("DELETE " in query or "TRUNCATE " in query for query in sql))

    @patch("pipelines.orchestration.movie_initial_load.load_exchange")
    @patch("pipelines.orchestration.movie_initial_load.read_legacy_candidate_exchange")
    def test_conflicting_metadata_fails_before_database_write(self, read, load):
        read.return_value = self.source
        self.published["artifact_version"] = "different"
        with self.assertRaises(RuntimeError):
            self.invoke()
        self.connection.cursor.assert_not_called()
        load.assert_not_called()

    @patch("pipelines.orchestration.movie_initial_load.load_exchange", side_effect=RuntimeError("rollback"))
    @patch("pipelines.orchestration.movie_initial_load.read_legacy_candidate_exchange")
    def test_load_failure_propagates_instead_of_authorizing_serving(self, read, load):
        read.return_value = self.source
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            self.invoke(fail_after_movie_merge=True)
        self.assertTrue(load.call_args.kwargs["fail_after_movie_merge"])


if __name__ == "__main__":
    unittest.main()
