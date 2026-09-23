"""Phase 1: validated S3 Raw → PostgreSQL STG → dbt DW/Mart → optional service publish."""
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, Param, dag, get_current_context, task

BUCKET = 'amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an'
RAW_URI = f's3://{BUCKET}/manifests/movie_api_daily/v1/DAILY_READY'
RAW_ASSET = Asset(uri=RAW_URI, name='pop_talk_daily_movie_raw_ready')
DEFAULTS = {'retries':2,'retry_delay':timedelta(minutes=3),'execution_timeout':timedelta(hours=1)}


@dag(dag_id='pop_talk_warehouse', description='영화 STG 적재 · dbt 품질 검사 · 선택적 서비스 게시',
     schedule=[RAW_ASSET], start_date=pendulum.datetime(2026,9,23,tz='Asia/Seoul'),
     catchup=False,is_paused_upon_creation=True,max_active_runs=1,max_active_tasks=1,
     default_args=DEFAULTS,tags=['pop-talk','phase1','warehouse'],params={
         'ready_manifest_key':Param('',type='string',description='수동 재처리할 DAILY_READY S3 key'),
         'initial_success_manifest_key':Param('',type='string',description='초기 전체 수집 SUCCESS S3 key; READY와 동시 지정 불가'),
         'initial_snapshot_sha256':Param('',type='string',pattern=r'^(|[0-9a-f]{64})$',description='보존된 초기 영화 JSON의 SHA-256. 다른 원본 Params와 동시 지정 불가'),
         'publish_service':Param(False,type='boolean',description='dbt 품질 검사 통과 후 서비스에 게시'),
     })
def movie_warehouse():
    @task
    def resolve_inputs():
        from pipelines.orchestration.asset_contract import resolve_ready_inputs
        context=get_current_context()
        initial=context['params']['initial_success_manifest_key']
        snapshot=context['params'].get('initial_snapshot_sha256','')
        if snapshot:
            import re
            if (not re.fullmatch('[0-9a-f]{64}',snapshot) or initial
                    or context['params']['ready_manifest_key']
                    or str(context['dag_run'].run_type).split('.')[-1].lower() != 'manual'):
                raise ValueError('Provide exactly one initial snapshot in a manual run')
            return [{'kind':'legacy_snapshot','sha256':snapshot}]
        if initial:
            import re
            match=re.fullmatch(r'manifests/movie_api/v1/run_id=([0-9a-f]{24})/SUCCESS[.]json',initial)
            if (str(context['dag_run'].run_type).split('.')[-1].lower() != 'manual'
                    or context['params']['ready_manifest_key'] or not match):
                raise ValueError('Provide exactly one initial SUCCESS key in a manual run')
            return [{'kind':'initial','ready_manifest_key':initial,'raw_run_id':match.group(1)}]
        events=[]
        for asset, values in context.get('triggering_asset_events',{}).items():
            for event in values:
                events.append({'asset_uri':asset if isinstance(asset,str) else asset.uri,'source_dag_id':event.source_dag_id,
                               'source_run_id':event.source_run_id,'extra':event.extra})
        return resolve_ready_inputs(run_type=context['dag_run'].run_type,events=events,
            manual_ready_key=context['params']['ready_manifest_key'],
            expected_asset_uri=RAW_URI,expected_bucket=BUCKET,max_map_length=100)

    @task
    def load_stg(ready):
        from pipelines.platform.database import connect
        if ready.get('kind')=='legacy_snapshot':
            import json
            from pathlib import Path
            from pipelines.platform.ingest import load_legacy_snapshot
            folder=Path('/opt/airflow/initial')/ready['sha256']
            manifest=json.loads((folder/'manifest.json').read_text())
            if manifest['sha256']!=ready['sha256']:
                raise ValueError('Initial snapshot identity mismatch')
            with connect() as connection:
                return load_legacy_snapshot(connection,manifest,(folder/'movies_final.json').read_bytes(),
                    uri=f"file:///opt/airflow/initial/{ready['sha256']}/manifest.json")
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from pipelines.collectors.movie_raw import S3Store
        from pipelines.platform.ingest import read_bundle,load_movie_bundle
        store=S3Store(S3Hook(aws_conn_id='pop_talk_aws').get_conn(),BUCKET)
        with connect() as connection:
            return load_movie_bundle(connection,read_bundle(store,ready),
                uri=f"s3://{BUCKET}/{ready['ready_manifest_key']}",
                orchestration_run_id=get_current_context()['run_id'])

    @task
    def build_and_publish():
        from pipelines.platform.database import connect,service_connect
        from pipelines.platform.build import build_warehouse
        context=get_current_context()
        with connect() as connection:
            return build_warehouse(connection,orchestration_run_id=context['run_id'],
                publish=context['params']['publish_service'],service_factory=service_connect)

    load_stg.expand(ready=resolve_inputs()) >> build_and_publish()


@dag(dag_id='pop_talk_reviews_warehouse',description='서비스 리뷰 읽기 전용 스냅샷 · dbt 집계',
     schedule=None,start_date=pendulum.datetime(2026,9,23,tz='Asia/Seoul'),
     catchup=False,is_paused_upon_creation=True,max_active_runs=1,max_active_tasks=1,
     default_args=DEFAULTS,tags=['pop-talk','phase1','reviews'])
def review_warehouse():
    @task
    def load_review_snapshot():
        from pipelines.platform.database import connect,service_connect
        from pipelines.platform.reviews import load_reviews
        with connect() as connection:
            return load_reviews(connection,lambda:service_connect(readonly=True),
                orchestration_run_id=get_current_context()['run_id'])

    @task
    def build_review_marts():
        from pipelines.platform.database import connect
        from pipelines.platform.build import build_warehouse
        with connect() as connection:
            return build_warehouse(connection,orchestration_run_id=get_current_context()['run_id'])

    load_review_snapshot() >> build_review_marts()


movie_warehouse()
review_warehouse()
