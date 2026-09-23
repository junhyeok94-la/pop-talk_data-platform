"""Legacy daily candidate strategy, immutable raw output; no serving DB writes."""
import re
from datetime import date, timedelta

from pipelines.collectors.movie_raw import Collector, CollectionError, digest, packed


def daily_plan(params, run_id, collection_date):
    anchor = date.fromisoformat(collection_date)
    days, cap = int(params.get('days_back', 7)), int(params.get('max_movies', 0))
    pages = int(params.get('kmdb_max_pages', 3))
    if not 1 <= days <= 31 or not 0 <= cap <= 1000 or not 1 <= pages <= 10:
        raise CollectionError('Invalid daily collection bounds')
    scope = {'version': 1, 'collection_date': collection_date, 'days_back': days,
             'max_movies': cap, 'kmdb_max_pages': pages,
             'boxoffice_dates': [(anchor-timedelta(days=i)).strftime('%Y%m%d') for i in range(1, days+1)],
             'selection': 'legacy_recent_boxoffice_and_current_year_feature_first_page',
             'policy': 'retain raw before legacy exclusions; matching and eligibility belong to Silver'}
    token = digest(packed({'dag_run_id': run_id, 'scope': scope}))[:24]
    suffix = f'movie_api_daily/v1/collection_date={collection_date}/run_id={token}'
    return {'run_id': token, 'airflow_run_id': run_id, 'scope': scope,
            'raw_prefix': f'raw/{suffix}', 'manifest_prefix': f'manifests/{suffix}'}


class DailyCollector(Collector):
    def movie_list(self):
        if self.existing('movie_list'):
            return self.manifest_key('movie_list')
        self.boxoffice()
        candidates, objects = {}, []
        for ref in self.existing('boxoffice')['objects']:
            objects.append(ref)
            rows = self.store.get(ref['key'])['payload']['boxOfficeResult']['dailyBoxOfficeList']
            for row in rows:
                self.add_candidate(candidates, row)
        year = date.fromisoformat(self.plan['scope']['collection_date']).year
        data, ref = self.capture('kofic', 'movie/searchMovieList',
            {'openStartDt': str(year), 'openEndDt': str(year), 'curPage': 1,
             'itemPerPage': 100, 'movieTypeCd': '220101'})
        objects.append(ref)
        for row in data['movieListResult']['movieList']:
            self.add_candidate(candidates, row)
        cap = self.plan['scope']['max_movies']
        selected = dict(list(candidates.items())[:cap or None])
        return self.finish('movie_list', objects, movies=selected, movie_count=len(selected),
            candidate_count=len(candidates), complete=False,
            candidate_scope_complete=len(selected) == len(candidates),
            current_year_reported_total=int(data['movieListResult']['totCnt']),
            coverage='Daily candidate window only; not complete historical change capture')

    @staticmethod
    def add_candidate(candidates, row):
        code = str(row.get('movieCd', ''))
        if not re.fullmatch(r'[A-Za-z0-9]{1,32}', code):
            raise CollectionError('Invalid daily candidate movie ID')
        candidates.setdefault(code, row)

    def validate_run(self):
        key = super().validate_run()
        # Separate daily completion contract: catalog_complete intentionally remains false.
        if not self.existing('DAILY_READY'):
            listing = self.existing('movie_list')
            self.finish('DAILY_READY', [], raw_success_manifest=key,
                candidate_scope_complete=listing['candidate_scope_complete'],
                sample=not listing['candidate_scope_complete'], movie_count=listing['movie_count'])
        return self.manifest_key('DAILY_READY')
