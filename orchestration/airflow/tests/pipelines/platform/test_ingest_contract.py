import copy
import unittest

from pipelines.platform.ingest import initial_bundle, read_bundle, timestamp
from pipelines.transforms.movie_bronze_silver import transform_run
from tests.pipelines.transforms.test_movie_bronze_silver import fixture


class IngestContractTests(unittest.TestCase):
    def initial(self):
        _,success,stages,raw=copy.deepcopy(fixture())
        scope={'start_year':2026,'end_year':2026,'max_movies_per_year':0,'boxoffice_dates':['20260908']}
        success.update(scope=scope,catalog_complete=True)
        for stage in stages.values():
            stage['scope']=scope
        stages['movie_list'].update(complete=True,years=[{
            'year':2026,'complete':True,'selected':1,'reported_total':1}])
        return success,stages,raw

    def test_initial_complete_catalogue_uses_same_validation(self):
        self.assertEqual(len(transform_run(*initial_bundle(*self.initial()))['silver_movies']),1)

    def test_initial_sample_or_missing_year_is_rejected(self):
        args=self.initial()
        args[0]['scope']['max_movies_per_year']=1
        with self.assertRaisesRegex(ValueError,'sampled or incomplete'):
            initial_bundle(*args)
        args=self.initial()
        args[1]['movie_list']['years']=[]
        with self.assertRaises(ValueError):
            initial_bundle(*args)

    def test_source_time_requires_timezone(self):
        with self.assertRaises(ValueError):
            timestamp('2026-09-23T12:00:00')

    def test_event_and_body_must_identify_same_raw_run(self):
        class Store:
            def get(self,key):
                return fixture()[0]
        with self.assertRaisesRegex(ValueError,'event run identity'):
            read_bundle(Store(),{'ready_manifest_key':'key','raw_run_id':'different'})
