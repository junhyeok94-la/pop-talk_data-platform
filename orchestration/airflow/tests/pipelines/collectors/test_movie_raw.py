import copy
import unittest

import requests

from pipelines.collectors.movie_raw import ApiClient, CollectionError, Collector, make_plan, packed


class MemoryStore:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return copy.deepcopy(self.data.get(key))

    def put(self, key, value):
        if key in self.data and self.data[key] != value:
            raise CollectionError("Immutable object conflict")
        self.data[key] = copy.deepcopy(value)


class FakeApi:
    def __init__(self, total=2):
        self.total, self.calls, self.fail_detail = total, [], False

    def fetch(self, source, endpoint, params):
        self.calls.append((source, endpoint, params))
        if "searchMovieList" in endpoint:
            offset = (params["curPage"]-1)*100
            return {"movieListResult": {"totCnt": self.total, "movieList": [
                {"movieCd": str(i), "movieNm": f"Movie {i}"}
                for i in range(offset+1, min(offset+100, self.total)+1)]}}
        if "searchMovieInfo" in endpoint:
            if self.fail_detail and params["movieCd"] == "2":
                self.fail_detail = False
                raise CollectionError("simulated failure")
            return {"movieInfoResult": {"movieInfo": {"movieCd": params["movieCd"], "movieNm": "Movie", "staffs": [{"name": "original"}]}}}
        if source == "kmdb":
            return {"TotalCount": 0, "Data": []}
        return {"boxOfficeResult": {"dailyBoxOfficeList": []}}


def parameters(**kwargs):
    return {"start_year": 2026, "end_year": 2026, "max_movies_per_year": 10,
            "max_pages_per_year": 100, "kmdb_max_pages": 3, **kwargs}


class RawTests(unittest.TestCase):
    def setup_collector(self, total=2, **kwargs):
        store, api = MemoryStore(), FakeApi(total)
        return Collector(store, api, make_plan(parameters(**kwargs), "manual__test", "2026-09-09"))

    def test_complete_and_idempotent(self):
        c = self.setup_collector()
        for method in (c.movie_list, c.details, c.kmdb_candidates, c.boxoffice, c.validate_run):
            method()
        before, calls = copy.deepcopy(c.store.data), len(c.api.calls)
        for method in (c.movie_list, c.details, c.kmdb_candidates, c.boxoffice, c.validate_run):
            method()
        self.assertEqual(before, c.store.data)
        self.assertEqual(calls, len(c.api.calls))
        self.assertEqual(c.existing("kmdb_candidates")["outcomes"][0]["mapping_status"], "NO_RESULTS")

    def test_resume_after_partial_detail_failure(self):
        c = self.setup_collector()
        c.movie_list()
        c.api.fail_detail = True
        with self.assertRaises(CollectionError):
            c.details()
        self.assertIsNone(c.existing("details"))
        c.details()
        first_calls = [x for x in c.api.calls if x[2] == {"movieCd": "1"}]
        self.assertEqual(len(first_calls), 1)

    def test_full_pagination_and_sample_coverage(self):
        full = self.setup_collector(105, max_movies_per_year=0)
        full.movie_list()
        self.assertEqual(full.existing("movie_list")["movie_count"], 105)
        self.assertTrue(full.existing("movie_list")["complete"])
        sample = self.setup_collector(105)
        sample.movie_list()
        self.assertEqual(sample.existing("movie_list")["movie_count"], 10)
        self.assertFalse(sample.existing("movie_list")["complete"])

    def test_page_cap_is_failure_not_partial_success(self):
        c = self.setup_collector(105, max_movies_per_year=0, max_pages_per_year=1)
        with self.assertRaises(CollectionError):
            c.movie_list()
        self.assertIsNone(c.existing("movie_list"))

    def test_alphanumeric_kofic_id_is_preserved(self):
        c = self.setup_collector(total=1)
        c.api.fetch = lambda *args: {"movieListResult": {"totCnt": 1,
            "movieList": [{"movieCd": "2026A754", "movieNm": "Example"}]}}
        c.movie_list()
        self.assertIn("2026A754", c.existing("movie_list")["movies"])

    def test_raw_keeps_staffs(self):
        c = self.setup_collector()
        c.movie_list(); c.details()
        obj = c.store.get(c.existing("details")["objects"][0]["key"])
        self.assertIn("staffs", obj["payload"]["movieInfoResult"]["movieInfo"])

    def test_corruption_prevents_success(self):
        c = self.setup_collector()
        c.movie_list(); c.details(); c.kmdb_candidates(); c.boxoffice()
        key = c.existing("details")["objects"][0]["key"]
        c.store.data[key]["payload"] = {}
        with self.assertRaises(CollectionError):
            c.validate_run()
        self.assertIsNone(c.existing("SUCCESS"))

    def test_bad_params_and_plan_isolation(self):
        with self.assertRaises(CollectionError):
            make_plan(parameters(start_year=2027), "run", "2026-09-09")
        with self.assertRaises(CollectionError):
            make_plan(parameters(boxoffice_start="2026-09-09", boxoffice_end="2026-09-09"), "run", "2026-09-09")
        a = make_plan(parameters(), "run", "2026-09-09")
        b = make_plan(parameters(max_movies_per_year=0), "run", "2026-09-09")
        self.assertNotEqual(a["run_id"], b["run_id"])

    def test_malformed_is_not_empty(self):
        for payload in ({}, {"TotalCount": 1, "Data": []}, {"ErrorCode": 9, "TotalCount": 0, "Data": []}):
            with self.assertRaises(CollectionError):
                Collector.validate_response("kmdb", "search", payload)

    def test_kmdb_pagination_and_truncation(self):
        for pages, expected_truncated in ((1, 1), (2, 0)):
            c = self.setup_collector(total=1, kmdb_max_pages=pages)
            original = c.api.fetch
            def fetch(source, endpoint, params):
                if source != "kmdb":
                    return original(source, endpoint, params)
                offset = params["startCount"]
                return {"TotalCount": 101, "Data": [{"Result": [
                    {"DOCID": str(i)} for i in range(offset, min(offset+100, 101))]}]}
            c.api.fetch = fetch
            c.movie_list(); c.details(); c.kmdb_candidates()
            result = c.existing("kmdb_candidates")
            self.assertEqual(result["truncated_movies"], expected_truncated)
            self.assertEqual(result["outcomes"][0]["candidate_count"], 100 if pages == 1 else 101)

    def test_quota_fails_without_rotation(self):
        class Response:
            status_code = 429
        class Session:
            def __init__(self): self.calls = 0
            def get(self, *args, **kwargs):
                self.calls += 1
                return Response()
        session = Session()
        api = ApiClient("TOPSECRET", "OTHERSECRET", session=session, sleep=lambda _: None)
        with self.assertRaises(CollectionError):
            api.fetch("kofic", "https://example", {})
        self.assertEqual(session.calls, 1)

    def test_day_scope_keeps_zero_observations(self):
        c = self.setup_collector(boxoffice_start="2026-09-07", boxoffice_end="2026-09-08")
        c.boxoffice()
        result = c.existing("boxoffice")
        self.assertEqual(result["day_count"], 2)
        self.assertEqual(result["objects"][0]["row_count"], 0)

    def test_http_error_never_logs_secret_url(self):
        class BadSession:
            def get(self, *args, **kwargs):
                raise requests.ConnectionError("https://example?key=TOPSECRET")
        api = ApiClient("TOPSECRET", "OTHERSECRET", session=BadSession(), sleep=lambda _: None)
        with self.assertRaises(CollectionError) as caught:
            api.fetch("kofic", "https://example", {})
        self.assertNotIn("TOPSECRET", str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)

    def test_response_secret_redaction(self):
        class Response:
            status_code = 200
            content = packed({"echo": "TOPSECRET"})
        class Session:
            def get(self, *args, **kwargs):
                return Response()
        api = ApiClient("TOPSECRET", "OTHERSECRET", session=Session(), sleep=lambda _: None)
        self.assertEqual(api.fetch("kofic", "https://example", {})["echo"], "[REDACTED]")


if __name__ == "__main__":
    unittest.main()
