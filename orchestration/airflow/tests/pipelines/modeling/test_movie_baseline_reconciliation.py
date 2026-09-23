import hashlib
import json
import unittest

from pipelines.modeling.movie_baseline_reconciliation import (
    Parsed,
    compare_values,
    normalize_kofic_id,
    reconcile_movies,
    relation_impact_counts,
    SnapshotContractError,
    validate_legacy_snapshot,
)


class MovieBaselineReconciliationTest(unittest.TestCase):
    def test_id_normalization_preserves_leading_zero_and_case(self):
        self.assertEqual(normalize_kofic_id(" 001Ab "), Parsed("VALUE", "001Ab"))
        self.assertEqual(normalize_kofic_id(" "), Parsed("MISSING"))
        self.assertEqual(normalize_kofic_id("bad/id"), Parsed("INVALID"))
        self.assertEqual(normalize_kofic_id(True), Parsed("INVALID"))
        self.assertEqual(normalize_kofic_id(123), Parsed("INVALID"))

    def test_value_comparison_keeps_invalid_separate_from_missing(self):
        self.assertEqual(compare_values(Parsed("INVALID"), Parsed("MISSING")), "legacy_invalid")
        self.assertEqual(compare_values(Parsed("MISSING"), Parsed("VALUE", 1)), "service_only_value")
        self.assertEqual(compare_values(Parsed("VALUE", 1), Parsed("VALUE", 2)), "conflict")

    def test_reconciliation_is_exclusive_and_conserves_rows(self):
        legacy = [
            {"movie_cd": "A", "movie_nm": "같음", "open_dt": "20200101"},
            {"movie_cd": "B", "movie_nm": "중복1", "open_dt": "20200101"},
            {"movie_cd": "B", "movie_nm": "중복2", "open_dt": "20200101"},
            {"movie_cd": "C", "movie_nm": "레거시만", "open_dt": "20200101"},
            {"movie_cd": "", "movie_nm": "ID없음", "open_dt": "20200101"},
            {"movie_cd": "bad/id", "movie_nm": "ID오류", "open_dt": "20200101"},
        ]
        service = [
            {"id": 1, "kofic_movie_cd": "A", "title_ko": "같음", "release_date": "2020-01-01"},
            {"id": 2, "kofic_movie_cd": "B", "title_ko": "상대단일", "release_date": "2020-01-01"},
            {"id": 3, "kofic_movie_cd": "D", "title_ko": "서비스만", "release_date": "2020-01-01"},
        ]
        gold = [
            {"kofic_movie_cd": "A"}, {"kofic_movie_cd": "B"},
            {"kofic_movie_cd": "D"}, {"kofic_movie_cd": "E"},
        ]
        result = reconcile_movies(legacy, service, gold)
        self.assertEqual(sum(result["legacy"]["classification"].values()), len(legacy))
        self.assertEqual(sum(result["service"]["classification"].values()), len(service))
        self.assertEqual(sum(result["gold"]["classification"].values()), len(gold))
        self.assertEqual(result["legacy"]["classification"]["duplicate_ambiguous"], 2)
        self.assertEqual(result["service"]["classification"]["duplicate_ambiguous"], 1)
        self.assertEqual(result["id_group_counts"]["legacy_service_singleton_matches"], 1)
        self.assertEqual(result["gold"]["classification"]["gold_only"], 1)

    def test_invalid_date_and_boolean_are_reported(self):
        legacy = [{"movie_cd": "A", "movie_nm": "영화", "open_dt": "garbage2020xx01yy01", "kmdb_matched": "maybe"}]
        service = [{"id": 1, "kofic_movie_cd": "A", "title_ko": "영화", "release_date": None,
                    "kmdb_matched": False}]
        result = reconcile_movies(legacy, service, [])
        self.assertEqual(result["matched_field_comparison"]["release_date"]["legacy_invalid"], 1)
        self.assertEqual(result["matched_field_comparison"]["kmdb_matched"]["legacy_invalid"], 1)

    def test_compact_and_iso_dates_are_both_valid(self):
        legacy = [{"movie_cd": "A", "movie_nm": "영화", "open_dt": "20200101"}]
        service = [{"id": 1, "kofic_movie_cd": "A", "title_ko": "영화",
                    "release_date": "2020-01-01"}]
        result = reconcile_movies(legacy, service, [])
        self.assertEqual(result["matched_field_comparison"]["release_date"], {"equal": 1})

    def test_casefold_and_gold_duplicates_are_diagnostics_not_matches(self):
        legacy = [{"movie_cd": "Ab", "movie_nm": "A", "open_dt": "20200101"}]
        service = [{"id": 1, "kofic_movie_cd": "aB", "title_ko": "A", "release_date": "2020-01-01"}]
        gold = [{"kofic_movie_cd": "G"}, {"kofic_movie_cd": "G"}]
        result = reconcile_movies(legacy, service, gold)
        self.assertEqual(result["id_group_counts"]["casefold_collision_groups"], 1)
        self.assertEqual(result["legacy"]["classification"], {"singleton_only": 1})
        self.assertEqual(result["gold"]["classification"], {"duplicate_ambiguous": 2})

    def test_relation_impact_counts_orphans(self):
        result = relation_impact_counts(
            [{"id": 1}, {"id": 2}],
            [{"movie_id": 1}, {"movie_id": 1}, {"movie_id": 9}],
        )
        self.assertEqual(result, {"row_count": 3, "linked_movie_count": 1, "orphan_row_count": 1})

    def test_manifest_contract_rejects_checksum_and_record_count(self):
        body = json.dumps([{"movie_cd": "001"}], separators=(",", ":")).encode()
        manifest = {
            "schema_version": 1, "status": "SUCCESS", "bucket": "bucket",
            "key": "raw/legacy_snapshot/v1/sha256=x/movies.json",
            "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "record_count": 1,
        }
        self.assertEqual(validate_legacy_snapshot(manifest, body, expected_bucket="bucket")[0]["movie_cd"], "001")
        with self.assertRaisesRegex(SnapshotContractError, "SHA256_MISMATCH"):
            validate_legacy_snapshot({**manifest, "sha256": "0" * 64}, body, expected_bucket="bucket")
        with self.assertRaisesRegex(SnapshotContractError, "RECORD_COUNT_MISMATCH"):
            validate_legacy_snapshot({**manifest, "record_count": 2}, body, expected_bucket="bucket")

    def test_policy_and_boolean_fixture_is_stable(self):
        legacy = [{
            "movie_cd": "A", "movie_nm": "영화", "open_dt": "20191231",
            "genre_alt": "다큐멘터리", "company_nm": "", "director_nm": "",
            "kmdb_matched": "true",
        }]
        service = [{
            "id": 1, "kofic_movie_cd": "A", "title_ko": "영화", "release_date": "2019-12-31",
            "genres": ["다큐멘터리"], "production_companies": [], "directors": [],
            "kmdb_matched": True,
        }]
        result = reconcile_movies(legacy, service, [{"kofic_movie_cd": "A"}])
        self.assertEqual(result["matched_field_comparison"]["kmdb_matched"], {"equal": 1})
        self.assertEqual(result["legacy_policy"]["exclusion_reasons"], {
            "excluded_genre": 1, "release_date_before_2020": 1,
        })
        self.assertEqual(result["matched_policy_comparison"]["classification"], {
            "legacy_excluded:service_excluded": 1,
        })


if __name__ == "__main__":
    unittest.main()
