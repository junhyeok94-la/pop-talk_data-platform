"""실제 결과가 없거나 조회에 실패했을 때 성공 증거를 만들지 않는지 검증한다."""
import unittest
from unittest.mock import MagicMock

from pipelines.orchestration.model_relation_validation import validate_candidate_relation, fingerprint_candidate_relation


RELATION = "POP_TALK_DW_DEV.CANDIDATE.DIM_MOVIE__BABCDEF123456__A1__F2"


class CandidateRelationTests(unittest.TestCase):
    def fingerprint(self, rows):
        self.cursor.description = [("ID", 0), ("PAYLOAD", 5)]
        self.cursor.fetchmany.side_effect = [rows, []] if rows else [[]]
        return fingerprint_candidate_relation(self.connection, RELATION)

    def test_content_digest_ignores_row_and_json_key_order(self):
        first = self.fingerprint([(1, {"a": 1, "b": None}), (2, {"x": "한글"})])
        second = self.fingerprint([(2, {"x": "한글"}), (1, {"b": None, "a": 1})])
        self.assertEqual(first, second)
        self.assertEqual(first["row_count"], 2)

    def test_content_and_duplicate_changes_are_detected(self):
        first = self.fingerprint([(1, '"old"')])["content_sha256"]
        self.assertNotEqual(first, self.fingerprint([(1, '"new"')])["content_sha256"])
        self.assertNotEqual(first, self.fingerprint([(1, '"old"'), (1, '"old"')])["content_sha256"])
        self.assertEqual(self.fingerprint([])["row_count"], 0)

    def test_connector_json_strings_are_canonical_and_null_is_typed(self):
        self.assertEqual(self.fingerprint([(1, '{"a":1,"b":2}')]), self.fingerprint([(1, '{"b":2,"a":1}')]))
        self.assertNotEqual(self.fingerprint([(1, 'null')]), self.fingerprint([(1, None)]))

    def setUp(self):
        self.connection = MagicMock()
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.cursor.sfqid = "query-123"

    def test_count_and_query_id_are_preserved_including_empty_table(self):
        for count in (0, 123456789):
            with self.subTest(count=count):
                self.cursor.fetchone.return_value = (count,)
                result = validate_candidate_relation(self.connection, RELATION)
                self.assertEqual((result.row_count, result.query_id), (count, "query-123"))
                self.assertEqual(result.relation_id, RELATION)
        self.cursor.execute.assert_called_with(
            'SELECT COUNT(*) FROM "POP_TALK_DW_DEV"."CANDIDATE".'
            '"DIM_MOVIE__BABCDEF123456__A1__F2"'
        )

    def test_invalid_or_shared_relation_is_rejected_before_query(self):
        for name in ("POP_TALK_DW_DEV.DW.DIM_MOVIE", RELATION + "; DROP TABLE X", ""):
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate_candidate_relation(self.connection, name)
        self.connection.cursor.assert_not_called()

    def test_query_failure_is_not_converted_to_empty_success(self):
        self.cursor.execute.side_effect = RuntimeError("table missing or access denied")
        with self.assertRaisesRegex(RuntimeError, "table missing"):
            validate_candidate_relation(self.connection, RELATION)
        self.cursor.fetchone.assert_not_called()
        self.connection.cursor.return_value.__exit__.assert_called_once()

    def test_missing_evidence_is_rejected(self):
        for row, query_id in ((None, "query"), ((-1,), "query"), ((0,), None)):
            with self.subTest(row=row, query_id=query_id):
                self.cursor.fetchone.return_value = row
                self.cursor.sfqid = query_id
                with self.assertRaises(RuntimeError):
                    validate_candidate_relation(self.connection, RELATION)


if __name__ == "__main__":
    unittest.main()
