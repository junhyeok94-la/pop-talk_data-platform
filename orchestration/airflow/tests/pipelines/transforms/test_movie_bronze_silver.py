import copy
import unittest

from pipelines.transforms.movie_bronze_silver import (
    PUBLICATION_ORDER_SQL, TransformContractError, choose_kmdb_match, digest, packed,
    evaluate_kmdb_match, exchange_observations,
    legacy_exchange_observations, silver_movie, transform_legacy_snapshot, transform_run,
)


class PublicationContractTests(unittest.TestCase):
    def test_revision_precedes_completion_time_for_same_source(self):
        self.assertEqual(
            PUBLICATION_ORDER_SQL,
            "o.source_observed_at DESC, l.publication_revision DESC, l.completed_at DESC",
        )
        older_late_success = ("2026-09-09T00:00:00Z", 1, "2026-09-09T03:00:00Z")
        fixed_earlier_success = ("2026-09-09T00:00:00Z", 2, "2026-09-09T02:00:00Z")
        selected = max((older_late_success, fixed_earlier_success),
                       key=lambda row: (row[0], row[1], row[2]))
        self.assertEqual(fixed_earlier_success, selected)

    def test_exchange_rows_preserve_delta_observation_identity(self):
        output = {"silver_movies": [{"source_object_key": "movie.json"}],
                  "silver_boxoffice": [{"source_object_key": "box.json"}]}
        raw = {"movie.json": {"collected_at": "2026-09-09T01:00:00Z"},
               "box.json": {"collected_at": "2026-09-09T02:00:00Z"}}
        result = exchange_observations(output, raw, "manifests/ready.json", "a" * 64)
        self.assertEqual("2026-09-09T01:00:00Z",
                         result["silver_movies"][0]["source_observed_at"])
        self.assertEqual("manifests/ready.json",
                         result["silver_boxoffice"][0]["ready_manifest_key"])
        self.assertEqual("a" * 64,
                         result["silver_boxoffice"][0]["transform_version"])


def fixture():
    scope = {"collection_date": "2026-09-09", "version": 1,
             "boxoffice_dates": ["20260908"]}
    detail = {"movieCd": "2026A754", "movieNm": "테스트 영화", "movieNmEn": "Test Movie",
              "openDt": "20260908", "prdtYear": "2026", "showTm": "101",
              "typeNm": "장편", "prdtStatNm": "개봉", "genres": [{"genreNm": "드라마"}],
              "nations": [{"nationNm": "한국"}],
              "directors": [{"peopleNm": "홍길동", "peopleNmEn": "Hong"}],
              "actors": [{"peopleNm": "배우", "cast": "주연"}],
              "companys": [{"companyNm": "좋은영화사"}], "audits": [{"watchGradeNm": "12세"}]}
    kmdb = {"DOCID": "K1", "title": "!HS테스트 영화!HE", "titleEng": "Test Movie",
            "prodYear": "2025", "posters": "https://p/1|https://p/2",
            "stlls": "https://s/1", "plots": {"plot": [{"plotLang": "한국어", "plotText": " 줄거리 "}]},
            "keywords": "우정, 성장", "ratings": {"rating": [{"ratingMain": "12세"}]}}
    payloads = {
        "list": {"movieListResult": {"totCnt": 1, "movieList": [{"movieCd": "2026A754"}]}},
        "detail": {"movieInfoResult": {"movieInfo": detail}},
        "kmdb": {"TotalCount": 1, "Data": [{"Result": [kmdb]}]},
        "box": {"boxOfficeResult": {"dailyBoxOfficeList": [{"movieCd": "2026A754", "rank": "1",
                                                                "salesAmt": "1000", "audiCnt": "10", "audiAcc": "20"}]}},
    }
    meta = {"list": ("kofic", "movie/searchMovieList"), "detail": ("kofic", "movie/searchMovieInfo"),
            "kmdb": ("kmdb", "search"), "box": ("kofic", "boxoffice/searchDailyBoxOfficeList")}
    raw, refs = {}, {}
    for key, payload in payloads.items():
        source, resource = meta[key]
        sha = digest(payload)
        raw[key] = {"schema_version": 1, "run_id": "run1", "source": source, "resource": resource,
                    "request": {"targetDt": "20260908"} if key == "box" else {},
                    "collected_at": "2026-09-09T00:00:00Z", "payload_sha256": sha, "payload": payload}
        refs[key] = {"key": key, "sha256": sha}
    stages = {
        "movie_list": {"objects": [refs["list"]], "movies": {"2026A754": {}},
                       "movie_count": 1, "candidate_scope_complete": True},
        "details": {"objects": [{**refs["detail"], "movie_cd": "2026A754"}], "movie_count": 1},
        "kmdb_candidates": {"objects": [{**refs["kmdb"], "movie_cd": "2026A754"}],
                            "outcomes": [{"movie_cd": "2026A754", "truncated": False}],
                            "movie_count": 1},
        "boxoffice": {"objects": [{**refs["box"], "target_date": "20260908"}], "day_count": 1},
    }
    for name, value in stages.items():
        value.update({"schema_version": 1, "run_id": "run1", "scope": scope,
                      "stage": name, "status": "SUCCESS"})
    success = {"schema_version": 1, "run_id": "run1", "scope": scope, "status": "SUCCESS", "movie_count": 1,
               "stage_manifests": {name: name for name in stages}}
    ready = {"schema_version": 1, "run_id": "run1", "scope": scope, "stage": "DAILY_READY",
             "status": "SUCCESS", "sample": False, "candidate_scope_complete": True,
             "movie_count": 1, "raw_success_manifest": "success"}
    return ready, success, stages, raw


class TransformTests(unittest.TestCase):
    def test_full_transform_preserves_bronze_and_builds_silver(self):
        output = transform_run(*fixture())
        self.assertEqual(len(output["bronze"]), 4)
        self.assertEqual(output["silver_movies"][0]["kofic_movie_cd"], "2026A754")
        self.assertTrue(output["silver_movies"][0]["kmdb_matched"])
        self.assertTrue(output["silver_movies"][0]["policy_eligible"])
        self.assertEqual(output["quality"]["boxoffice_row_count"], 1)

    def test_sample_is_rejected_for_operational_transform(self):
        args = list(fixture())
        args[0]["sample"] = True
        with self.assertRaisesRegex(TransformContractError, "Sample READY"):
            transform_run(*args)
        self.assertEqual(transform_run(*args, allow_sample=True)["quality"]["movie_count"], 1)

    def test_checksum_mismatch_fails_closed(self):
        args = list(fixture())
        args[3]["detail"]["payload"]["movieInfoResult"]["movieInfo"]["movieNm"] = "변조"
        with self.assertRaisesRegex(TransformContractError, "checksum mismatch"):
            transform_run(*args)

    def test_scope_mismatch_fails_closed(self):
        args = list(fixture())
        args[2]["details"]["scope"] = {"other": True}
        with self.assertRaisesRegex(TransformContractError, "scope mismatch"):
            transform_run(*args)

    def test_equal_best_candidates_are_ambiguous_not_arbitrarily_matched(self):
        detail = {"movieNm": "영화", "movieNmEn": "Movie", "prdtYear": "2026"}
        candidates = [{"DOCID": "B", "title": "영화", "prodYear": "2025"},
                      {"DOCID": "A", "title": "영화", "prodYear": "2025"},
                      {"DOCID": "C", "titleEng": "Movie", "prodYear": "2026"}]
        decision = evaluate_kmdb_match(detail, candidates)
        self.assertEqual(decision["status"], "REVIEW_REQUIRED")
        self.assertTrue(decision["ambiguous"])
        self.assertIsNone(choose_kmdb_match(detail, candidates))

    def test_duplicate_candidate_from_two_queries_is_deduplicated(self):
        detail = {"movieNm": "영화", "prdtYear": "2026"}
        candidate = {"DOCID": "A", "title": "영화", "prodYear": "2026"}
        decision = evaluate_kmdb_match(detail, [candidate, copy.deepcopy(candidate)])
        self.assertEqual(decision["status"], "MATCHED")
        self.assertEqual(decision["candidate_count"], 1)

    def test_far_year_or_different_title_is_not_matched(self):
        detail = {"movieNm": "영화", "prdtYear": "2026"}
        self.assertIsNone(choose_kmdb_match(detail, [{"title": "영화", "prodYear": "2010"}]))
        self.assertIsNone(choose_kmdb_match(detail, [{"title": "다른 영화", "prodYear": "2026"}]))

    def test_policy_marks_documentary_without_dropping_row(self):
        detail = fixture()[3]["detail"]["payload"]["movieInfoResult"]["movieInfo"]
        detail = copy.deepcopy(detail)
        detail["genres"] = [{"genreNm": "다큐멘터리"}]
        row = silver_movie(detail, [], run_id="run1", source_object_key="detail", kmdb_truncated=False)
        self.assertFalse(row["policy_eligible"])
        self.assertIn("excluded_genre", row["policy_exclusion_reasons"])

    def test_truncated_unmatched_candidates_require_review(self):
        detail = fixture()[3]["detail"]["payload"]["movieInfoResult"]["movieInfo"]
        row = silver_movie(detail, [{"title": "다름", "prodYear": "2026"}], run_id="run1",
                           source_object_key="detail", kmdb_truncated=True)
        self.assertEqual(row["kmdb_mapping_status"], "REVIEW_REQUIRED")

    def test_truncated_best_candidate_is_only_provisional(self):
        detail = fixture()[3]["detail"]["payload"]["movieInfoResult"]["movieInfo"]
        candidate = fixture()[3]["kmdb"]["payload"]["Data"][0]["Result"][0]
        row = silver_movie(detail, [candidate], run_id="run1", source_object_key="detail",
                           kmdb_truncated=True)
        self.assertFalse(row["kmdb_matched"])
        self.assertEqual(row["kmdb_mapping_status"], "REVIEW_REQUIRED")
        self.assertEqual(row["provisional_kmdb_id"], "K1")
        self.assertIsNone(row["plot"])

    def test_content_hash_ignores_observation_but_detects_business_change(self):
        detail = fixture()[3]["detail"]["payload"]["movieInfoResult"]["movieInfo"]
        first = silver_movie(detail, [], run_id="day1", source_object_key="one", kmdb_truncated=False)
        second = silver_movie(detail, [], run_id="day2", source_object_key="two", kmdb_truncated=False)
        self.assertEqual(first["content_sha256"], second["content_sha256"])
        changed = copy.deepcopy(detail)
        changed["movieNm"] = "수정된 영화"
        third = silver_movie(changed, [], run_id="day3", source_object_key="three", kmdb_truncated=False)
        self.assertNotEqual(first["content_sha256"], third["content_sha256"])

    def test_matching_diagnostics_have_a_separate_hash(self):
        detail = fixture()[3]["detail"]["payload"]["movieInfoResult"]["movieInfo"]
        complete = silver_movie(detail, [], run_id="day1", source_object_key="one", kmdb_truncated=False)
        truncated = silver_movie(detail, [{"DOCID": "X", "title": "다른 영화", "prodYear": "2026"}],
                                 run_id="day2", source_object_key="two", kmdb_truncated=True)
        self.assertEqual(complete["content_sha256"], truncated["content_sha256"])
        self.assertNotEqual(complete["matching_sha256"], truncated["matching_sha256"])

    def test_declared_movie_sets_and_counts_are_reconciled(self):
        args = list(fixture())
        args[1]["movie_count"] = 999
        with self.assertRaisesRegex(TransformContractError, "movie count mismatch"):
            transform_run(*args)
        args = list(fixture())
        args[2]["movie_list"]["movies"] = {"OTHER": {}}
        with self.assertRaisesRegex(TransformContractError, "ID sets differ"):
            transform_run(*args)

    def test_duplicate_outcome_and_boxoffice_date_mismatch_fail(self):
        args = list(fixture())
        args[2]["kmdb_candidates"]["outcomes"].append(
            copy.deepcopy(args[2]["kmdb_candidates"]["outcomes"][0]))
        with self.assertRaisesRegex(TransformContractError, "Duplicate KMDb outcome"):
            transform_run(*args)
        args = list(fixture())
        args[3]["box"]["request"] = {"targetDt": "20260907"}
        with self.assertRaisesRegex(TransformContractError, "reference/request date mismatch"):
            transform_run(*args)

    def test_boxoffice_business_key_is_unique_and_hashed(self):
        output = transform_run(*fixture())
        row = output["silver_boxoffice"][0]
        self.assertEqual(len(row["observation_sha256"]), 64)
        args = list(fixture())
        items = args[3]["box"]["payload"]["boxOfficeResult"]["dailyBoxOfficeList"]
        items.append(copy.deepcopy(items[0]))
        args[3]["box"]["payload_sha256"] = digest(args[3]["box"]["payload"])
        args[2]["boxoffice"]["objects"][0]["sha256"] = args[3]["box"]["payload_sha256"]
        with self.assertRaisesRegex(TransformContractError, "Duplicate boxoffice movie"):
            transform_run(*args)

    def test_legacy_snapshot_adapter_uses_common_identity_and_stable_hash(self):
        records = [{"movie_cd": "2026A754", "kmdb_id": "K1", "kmdb_matched": True,
                    "movie_nm": "영화", "open_dt": "20260908", "prdt_year": "2026",
                    "genre_alt": "드라마,가족", "nation_alt": "한국,미국",
                    "director_nm": "감독1|감독2", "actor_nm": "배우1|배우2",
                    "company_nm": "회사1|회사2", "keywords": "우정|성장",
                    "posters_all": ["p1", "p2"], "stlls_all": ["s1"]}]
        import json
        body = json.dumps(records, ensure_ascii=False).encode("utf-8")
        manifest = {"schema_version": 1, "status": "SUCCESS", "key": "raw/legacy.json",
                    "sha256": digest(body), "bytes": len(body), "record_count": 1}
        row = transform_legacy_snapshot(manifest, body)["silver_movies"][0]
        self.assertEqual(row["canonical_movie_key"], "kofic:2026A754")
        self.assertIsNone(row["service_movie_id"])
        self.assertEqual(row["kmdb_mapping_status"], "LEGACY_ACCEPTED")
        self.assertEqual(row["genres"], ["드라마", "가족"])
        self.assertEqual(row["directors"], ["감독1", "감독2"])
        self.assertEqual(row["posters"], ["p1", "p2"])

    def test_daily_and_legacy_same_business_content_have_same_hash(self):
        detail = fixture()[3]["detail"]["payload"]["movieInfoResult"]["movieInfo"]
        daily = silver_movie(detail, [], run_id="daily", source_object_key="detail",
                             kmdb_truncated=False)
        record = {"movie_cd": "2026A754", "kmdb_id": "", "kmdb_matched": False,
                  "movie_nm": "테스트 영화", "movie_nm_en": "Test Movie", "movie_nm_og": "",
                  "open_dt": "20260908", "prdt_year": "2026", "show_tm": "101",
                  "type_nm": "장편", "prdt_stat_nm": "개봉", "nation_alt": "한국",
                  "genre_alt": "드라마", "director_nm": "홍길동", "director_nm_en": "Hong",
                  "actor_nm": "배우", "actor_cast": "주연", "company_nm": "좋은영화사",
                  "watch_grade": "12세", "poster_url": "", "posters_all": [], "stlls_all": [],
                  "plot": "", "keywords": ""}
        import json
        body = json.dumps([record], ensure_ascii=False).encode("utf-8")
        manifest = {"schema_version": 1, "status": "SUCCESS", "key": "raw/legacy.json",
                    "sha256": digest(body), "bytes": len(body), "record_count": 1}
        legacy = transform_legacy_snapshot(manifest, body)["silver_movies"][0]
        self.assertEqual(daily["content_sha256"], legacy["content_sha256"])

    def test_legacy_snapshot_checksum_is_required(self):
        body = b"[]"
        manifest = {"schema_version": 1, "status": "SUCCESS", "sha256": "wrong",
                    "bytes": len(body), "record_count": 0}
        with self.assertRaisesRegex(TransformContractError, "checksum mismatch"):
            transform_legacy_snapshot(manifest, body)

    def test_legacy_exchange_v2_has_unknown_source_time_and_empty_boxoffice(self):
        body = packed([{"movie_cd": "1", "movie_nm": "영화", "open_dt": "20260101"}])
        manifest = {
            "schema_version": 1, "status": "SUCCESS", "key": "raw/legacy.json",
            "bytes": len(body), "sha256": digest(body), "record_count": 1,
        }
        output = transform_legacy_snapshot(manifest, body)
        exchange = legacy_exchange_observations(
            output, manifest_key="manifests/legacy.json", transform_version="a" * 64,
        )
        row = exchange["silver_movies"][0]
        self.assertEqual(
            (row["source_observed_at"], row["source_observed_at_known"],
             row["source_time_basis"]),
            (None, False, "LEGACY_UNKNOWN"),
        )
        self.assertEqual(exchange["silver_boxoffice"], [])


if __name__ == "__main__":
    unittest.main()
