"""Integration scenarios in an explicitly named disposable database, never the service DB.

RUN_PHASE1_POSTGRES_TESTS=1 and POP_TALK_PLATFORM_POSTGRES_DB=phase1_test_<suffix>.
The dev fixture covers SQL contracts; application-owned triggers need service integration QA.
"""
import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid

import psycopg
from psycopg import sql

from pipelines.platform.database import connect, warehouse_lock
from pipelines.platform.ingest import initial_bundle, load_movie_bundle, load_legacy_snapshot
from pipelines.platform.build import build_warehouse, publish_frozen
from pipelines.platform.reviews import load_reviews
from pipelines.orchestration.service_sync import FIELDS, ARRAYS, sync_service
from pipelines.transforms.movie_bronze_silver import digest
from tests.pipelines.transforms.test_movie_bronze_silver import fixture
from tests.pipelines.platform import test_ingest_contract as ingest_contract


def changed_bundle(day='2026-09-10', title='Updated', audience='15', code='2026A754'):
    bundle=copy.deepcopy(fixture())
    raw=bundle[3]
    raw['detail']['payload']['movieInfoResult']['movieInfo']['movieNm']=title
    box=raw['box']['payload']['boxOfficeResult']['dailyBoxOfficeList'][0]
    box.update(movieCd=code, audiCnt=audience)
    for key,envelope in raw.items():
        envelope['collected_at']=day+'T00:00:00Z'
        envelope['payload_sha256']=digest(envelope['payload'])
        for stage in bundle[2].values():
            for ref in stage['objects']:
                if ref['key']==key:
                    ref['sha256']=envelope['payload_sha256']
    return bundle


@unittest.skipUnless(os.environ.get('RUN_PHASE1_POSTGRES_TESTS')=='1','Disposable Phase 1 PostgreSQL integration')
class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        name=os.environ['POP_TALK_PLATFORM_POSTGRES_DB']
        if not name.startswith('phase1_test_') or not name.removeprefix('phase1_test_').isalnum():
            raise RuntimeError('Integration tests require an isolated phase1_test_* database')
        cls.db=connect()
        cls.db.execute(Path('/opt/airflow/modules/pipelines/platform/schema.sql').read_text())
        cls.db.execute('CREATE SCHEMA dev')
        columns=[]
        for field in FIELDS:
            kind='text[]' if field in ARRAYS else ('boolean' if field=='kmdb_matched' else 'text')
            columns.append(sql.SQL('{} {}').format(sql.Identifier(field),sql.SQL(kind)))
        cls.db.execute(sql.SQL('''CREATE TABLE dev.popcorn_movies (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, kofic_movie_cd text UNIQUE NOT NULL,
            {}, source_hash text, source_synced_at timestamptz,
            service_status text NOT NULL DEFAULT 'DRAFT', approval_status text NOT NULL DEFAULT 'PENDING')''').format(sql.SQL(',').join(columns)))
        cls.db.execute('''CREATE TABLE dev.movie_boxoffice_daily (
            kofic_movie_cd text, target_date date, movie_id bigint REFERENCES dev.popcorn_movies,
            rank integer CHECK(rank>0),audience_count bigint CHECK(audience_count>=0),
            audience_accumulated bigint,sales_amount bigint,source_observed_at timestamptz,
            source_run_id text,updated_at timestamptz DEFAULT now(),PRIMARY KEY(kofic_movie_cd,target_date))''')
        cls.db.execute('''CREATE TABLE dev.batch_runs (
            job_name text,scheduled_for timestamptz,status text,source_file text,source_hash text,
            processed_count integer,inserted_count integer,updated_count integer,failed_count integer,
            result jsonb,last_error text,started_at timestamptz,finished_at timestamptz,
            PRIMARY KEY(job_name,scheduled_for))''')
        cls.db.execute('''CREATE TABLE dev.reviews (
            id uuid PRIMARY KEY, movie_id bigint REFERENCES dev.popcorn_movies, source_system text,
            source_review_key text,rating numeric(2,1),status text,created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now(),deleted_at timestamptz)''')

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def setUp(self):
        self.db.execute('''TRUNCATE stg.movie_observations,stg.boxoffice_observations,
            stg.review_observations,ops.source_loads,ops.service_publications,
            dev.reviews,dev.popcorn_movies,dev.movie_boxoffice_daily,dev.batch_runs RESTART IDENTITY''')

    def load(self,bundle=None,uri='s3://fixture/one'):
        return load_movie_bundle(self.db,bundle or fixture(),uri=uri,orchestration_run_id='integration')

    def build(self,**kwargs):
        return build_warehouse(self.db,orchestration_run_id=kwargs.pop('run','integration'),
            artifact_root='/opt/airflow/logs/phase1-integration',**kwargs)

    def test_atomic_idempotent_load_and_lock(self):
        first=self.load()
        self.assertTrue(first['created'])
        self.assertFalse(self.load()['created'])
        with self.assertRaisesRegex(ValueError,'Immutable'):
            self.load(changed_bundle())
        with self.assertRaises(psycopg.errors.NumericValueOutOfRange):
            self.load(changed_bundle(audience='9'*50),uri='s3://fixture/overflow')
        self.assertEqual(self.db.execute('SELECT count(*) FROM ops.source_loads').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM stg.movie_observations').fetchone()[0],1)
        with connect() as other, warehouse_lock(self.db):
            with self.assertRaisesRegex(RuntimeError,'Another'):
                with warehouse_lock(other):
                    self.fail('Second session must not acquire the lock')

    def test_backfill_does_not_replace_newer_observation(self):
        self.load(changed_bundle(),uri='s3://fixture/newer')
        self.load()
        result=self.build()
        self.assertGreater(result['dbt_checks'],8)
        self.assertEqual(self.db.execute('SELECT title_ko FROM dw.dim_movie').fetchone()[0],'Updated')
        self.assertEqual(self.db.execute('SELECT count(*),sum(audience_count) FROM dw.fct_boxoffice_daily').fetchone(),(1,15))
        self.assertEqual(self.db.execute('SELECT latest_audience_accumulated FROM mart.movie_performance').fetchone()[0],20)

    def test_initial_load_contract_is_registered(self):
        self.load(initial_bundle(*ingest_contract.IngestContractTests().initial()),uri='s3://fixture/initial')
        self.assertEqual(self.db.execute('SELECT source_kind FROM ops.source_loads').fetchone()[0],'movie_initial')

    def test_legacy_snapshot_is_idempotent_and_does_not_override_dated_source(self):
        body=json.dumps([{'movie_cd':'2026A754','movie_nm':'Legacy','open_dt':'20260908'}]).encode()
        manifest=dict(schema_version=1,status='SUCCESS',key='file:///initial.json',
                      sha256=digest(body),bytes=len(body),record_count=1)
        self.assertTrue(load_legacy_snapshot(self.db,manifest,body,uri='file:///manifest.json')['created'])
        self.build()
        self.assertEqual(self.db.execute('SELECT title_ko,source_observed_at FROM dw.dim_movie').fetchone(),('Legacy',None))
        self.load(changed_bundle())
        self.assertFalse(load_legacy_snapshot(self.db,manifest,body,uri='file:///manifest.json')['created'])
        self.build()
        self.assertEqual(self.db.execute('SELECT title_ko FROM dw.dim_movie').fetchone()[0],'Updated')

    def test_conflicting_observation_time_blocks_build(self):
        self.load()
        self.load(changed_bundle(day='2026-09-09'),uri='s3://fixture/conflict')
        with self.assertRaisesRegex(RuntimeError,'dbt build/test failed'):
            self.build()
        self.load(changed_bundle(day='2026-09-10'),uri='s3://fixture/resolved')
        self.build()
        self.assertEqual(self.db.execute('SELECT title_ko FROM dw.dim_movie').fetchone()[0],'Updated')

    def test_unmapped_boxoffice_blocks_publication(self):
        self.load(changed_bundle(code='UNKNOWN'))
        def forbidden():
            self.fail('Service connection must not open before dbt quality gate passes')
        with self.assertRaisesRegex(RuntimeError,'dbt build/test failed'):
            self.build(publish=True,service_factory=forbidden)
        self.assertEqual(self.db.execute('SELECT count(*) FROM ops.service_publications').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM stg.boxoffice_observations').fetchone()[0],1)

    def test_review_status_and_hard_delete_propagation(self):
        self.load()
        self.db.execute("INSERT INTO dev.popcorn_movies(kofic_movie_cd,title_ko) VALUES ('2026A754','Service')")
        internal,external=uuid.uuid4(),uuid.uuid4()
        self.db.execute("INSERT INTO dev.reviews(id,movie_id,rating,status) VALUES (%s,1,4.5,'ACTIVE')",(internal,))
        self.db.execute("INSERT INTO dev.reviews(id,movie_id,rating,status,source_system,source_review_key) VALUES (%s,1,3,'ACTIVE','external',%s)",(external,'a'*64))
        load_reviews(self.db,connect,orchestration_run_id='snapshot1')
        self.assertFalse(load_reviews(self.db,connect,orchestration_run_id='snapshot1')['created'])
        self.build()
        self.assertEqual(self.db.execute('SELECT count(*) FROM mart.movie_review_summary').fetchone()[0],2)
        self.assertIsNone(self.db.execute("SELECT original_reviewed_at FROM dw.fct_review WHERE source_system='external:external'").fetchone()[0])
        self.db.execute("UPDATE dev.reviews SET status='HIDDEN',updated_at=now() WHERE id=%s",(internal,))
        self.db.execute('DELETE FROM dev.reviews WHERE id=%s',(external,))
        load_reviews(self.db,connect,orchestration_run_id='snapshot2')
        self.build()
        self.assertEqual(self.db.execute('SELECT review_count,average_rating FROM mart.movie_review_summary').fetchone(),(0,None))
        self.db.execute('DELETE FROM dev.reviews')
        load_reviews(self.db,connect,orchestration_run_id='snapshot3')
        self.build()
        self.assertEqual(self.db.execute('SELECT count(*) FROM dw.fct_review').fetchone()[0],0)

    def test_publication_response_loss_reconciles_and_preserves_service_id(self):
        self.load()
        self.db.execute("""INSERT INTO dev.popcorn_movies(kofic_movie_cd,title_ko,service_status,approval_status)
            VALUES ('2026A754','Before','PUBLISHED','APPROVED')""")
        def lost_response(*args,**kwargs):
            sync_service(*args,**kwargs)
            raise ConnectionError('Simulated response loss after service commit')
        with patch('pipelines.platform.build.sync_service',side_effect=lost_response):
            with self.assertRaises(ConnectionError):
                self.build(run='publish1',publish=True,service_factory=connect)
        self.assertEqual(self.db.execute('SELECT status FROM dev.batch_runs').fetchone()[0],'SUCCEEDED')
        self.build(run='publish1',publish=True,service_factory=connect)
        self.assertEqual(self.db.execute('SELECT status FROM ops.service_publications').fetchone()[0],'SUCCEEDED')
        self.assertEqual(self.db.execute('SELECT id,service_status,approval_status FROM dev.popcorn_movies').fetchone(),(1,'PUBLISHED','APPROVED'))
        old=self.db.execute('SELECT publication_id FROM ops.service_publications').fetchone()[0]
        self.load(changed_bundle(),uri='s3://fixture/newer')
        self.build(run='publish2',publish=True,service_factory=connect)
        self.db.execute("UPDATE ops.service_publications SET status='FAILED' WHERE publication_id=%s",(old,))
        publish_frozen(self.db,connect,old)
        self.assertEqual(self.db.execute('SELECT title_ko FROM dev.popcorn_movies').fetchone()[0],'Updated')
        self.assertEqual(self.db.execute('SELECT count(*) FROM dev.movie_boxoffice_daily').fetchone()[0],1)


if __name__=='__main__':
    unittest.main()
