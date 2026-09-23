"""Read-only daily S3 verification, using Airflow's registered AWS connection."""
import json
import sys
import boto3
from airflow.models.connection import Connection
from airflow.utils.session import create_session

sys.path.insert(0, '/opt/airflow/modules')
from pipelines.collectors.movie_raw import S3Store

bucket = 'amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an'
prefix = 'manifests/movie_api_daily/v1/collection_date=' + sys.argv[1] + '/'
with create_session() as session:
    conn = session.query(Connection).filter(Connection.conn_id == 'pop_talk_aws').one()
    client = boto3.client('s3', aws_access_key_id=conn.login, aws_secret_access_key=conn.password,
                         region_name=conn.extra_dejson.get('region_name', 'ap-northeast-2'))
store = S3Store(client, bucket)
results = []
for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
    for item in page.get('Contents', []):
        if item['Key'].endswith('/DAILY_READY.json'):
            ready = store.get(item['Key'])
            success = store.get(ready['raw_success_manifest'])
            results.append({'key': item['Key'], 'airflow_run_id': ready['airflow_run_id'],
                'movie_count': ready['movie_count'], 'sample': ready['sample'],
                'candidate_scope_complete': ready['candidate_scope_complete'],
                'kmdb_truncated_movies': success['kmdb_truncated_movies']})
print(json.dumps(results, ensure_ascii=False))
