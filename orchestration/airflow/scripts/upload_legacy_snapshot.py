"""Run in scheduler; source bytes arrive on stdin, never rewrite the dataset."""
import hashlib
import json
import sys

import boto3
from airflow.models.connection import Connection
from airflow.utils.session import create_session
from botocore.exceptions import ClientError

BUCKET = 'amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an'


def main():
    import base64
    body = base64.b64decode(sys.stdin.buffer.read().strip(), validate=True)
    dataset = json.loads(body)
    if not isinstance(dataset, (list, dict)) or not dataset:
        raise ValueError('Expected a nonempty legacy dataset')
    sha = hashlib.sha256(body).hexdigest()
    key = f'raw/legacy_snapshot/v1/sha256={sha}/movies_final.json'
    with create_session() as session:
        conn = session.query(Connection).filter(Connection.conn_id == 'pop_talk_aws').one()
        client = boto3.client('s3', aws_access_key_id=conn.login, aws_secret_access_key=conn.password,
                             region_name=conn.extra_dejson.get('region_name', 'ap-northeast-2'))
    def put_exact(target, content):
        checksum = hashlib.sha256(content).hexdigest()
        try:
            client.put_object(Bucket=BUCKET, Key=target, Body=content, ContentType='application/json',
                              Metadata={'sha256': checksum}, IfNoneMatch='*')
        except ClientError as exc:
            if exc.response['Error']['Code'] not in ('412', 'PreconditionFailed'):
                raise
        remote = client.get_object(Bucket=BUCKET, Key=target)['Body'].read()
        if remote != content:
            raise ValueError('Snapshot readback differs from original bytes')
    put_exact(key, body)
    manifest = {'schema_version': 1, 'status': 'SUCCESS', 'bucket': BUCKET, 'key': key,
                'sha256': sha, 'bytes': len(body), 'record_count': len(dataset),
                'source': 'pop_talk-local_dev/batch/initial_dataset/data/movies_final.json',
                'policy': 'Original file bytes preserved; serving PostgreSQL not overwritten'}
    manifest_key = f'manifests/legacy_snapshot/v1/sha256={sha}/SUCCESS.json'
    put_exact(manifest_key, json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode('utf-8'))
    print(json.dumps({**manifest, 'manifest_key': manifest_key}, ensure_ascii=False))


if __name__ == '__main__':
    main()
