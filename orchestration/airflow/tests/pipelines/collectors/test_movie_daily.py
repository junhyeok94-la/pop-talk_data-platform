import unittest
from pipelines.collectors.movie_daily import DailyCollector, daily_plan
from .test_movie_raw import MemoryStore, FakeApi


class DailyTests(unittest.TestCase):
    def test_window_rolls_across_year_and_replay_has_no_calls(self):
        plan = daily_plan({'days_back': 7, 'max_movies': 1}, 'daily-test', '2026-01-01')
        self.assertEqual(plan['scope']['boxoffice_dates'][0], '20251231')
        self.assertEqual(plan['scope']['boxoffice_dates'][-1], '20251225')
        c = DailyCollector(MemoryStore(), FakeApi(105), plan)
        for method in (c.boxoffice, c.movie_list, c.details, c.kmdb_candidates, c.validate_run):
            method()
        calls = len(c.api.calls)
        for method in (c.boxoffice, c.movie_list, c.details, c.kmdb_candidates, c.validate_run):
            method()
        self.assertEqual(calls, len(c.api.calls))
        self.assertTrue(c.existing('DAILY_READY')['sample'])
        self.assertFalse(c.existing('SUCCESS')['catalog_complete'])

    def test_daily_completion_does_not_claim_historical_completeness(self):
        c = DailyCollector(MemoryStore(), FakeApi(105), daily_plan({}, 'test', '2026-09-09'))
        c.movie_list()
        self.assertEqual(c.existing('movie_list')['movie_count'], 100)
        self.assertTrue(c.existing('movie_list')['candidate_scope_complete'])
        self.assertFalse(c.existing('movie_list')['complete'])
        self.assertNotEqual(c.plan['raw_prefix'], daily_plan({}, 'other', '2026-09-09')['raw_prefix'])

    def test_boxoffice_union_deduplicates_and_preserves_alphanumeric_ids(self):
        rows = {}
        DailyCollector.add_candidate(rows, {'movieCd': '2026A754', 'movieNm': 'first'})
        DailyCollector.add_candidate(rows, {'movieCd': '2026A754', 'movieNm': 'second'})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows['2026A754']['movieNm'], 'first')
