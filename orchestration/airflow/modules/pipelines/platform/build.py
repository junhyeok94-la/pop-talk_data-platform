"""Build/test dbt and freeze service publication input while holding the warehouse lock."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipelines.orchestration.service_sync import sync_service
from pipelines.platform.database import warehouse_lock
from pipelines.transforms.movie_bronze_silver import digest


def model_version(project):
    checksum = hashlib.sha256()
    for path in sorted(project.rglob('*')):
        if path.is_file() and path.suffix in ('.sql','.yml') and not {'target','logs','dbt_packages'} & set(path.parts):
            checksum.update(str(path.relative_to(project)).encode())
            checksum.update(path.read_bytes())
    return checksum.hexdigest()


def run_dbt(*, artifact_root=None):
    project = Path(os.environ.get('POP_TALK_DBT_PROJECT_DIR','/opt/airflow/dbt'))
    artifacts = Path(artifact_root or '/opt/airflow/logs/phase1-dbt') / uuid.uuid4().hex
    artifacts.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.pop('PYTHONPATH', None)
    env.update(DBT_SEND_ANONYMOUS_USAGE_STATS='false', DBT_USE_COLORS='false')
    command = [os.environ.get('POP_TALK_DBT_BIN','/home/airflow/dbt-venv/bin/dbt'), 'build',
        '--project-dir', str(project), '--profiles-dir',str(project),
        '--target-path',str(artifacts/'target'), '--log-path',str(artifacts/'logs'), '--no-partial-parse']
    with (artifacts/'console.log').open('w') as output:
        result = subprocess.run(command,env=env,stdout=output,stderr=subprocess.STDOUT,timeout=1800,check=False)
    if result.returncode:
        raise RuntimeError(f'dbt build/test failed; inspect {artifacts}/console.log')
    results = json.loads((artifacts/'target/run_results.json').read_text())
    if not results['results'] or any(item['status'] not in ('success','pass') for item in results['results']):
        raise RuntimeError('dbt did not pass every model and quality test')
    return {'model_version':model_version(project),'artifacts':str(artifacts),'dbt_checks':len(results['results'])}


def freeze_publication(connection, orchestration_run_id, version):
    publication_id = digest(['pop_talk',orchestration_run_id])
    with connection.cursor(row_factory=dict_row) as cursor:
        movies = [row['attributes'] for row in cursor.execute('SELECT attributes FROM dw.dim_movie ORDER BY kofic_movie_cd')]
        facts = list(cursor.execute('''SELECT kofic_movie_cd,target_date,rank,audience_count,
            audience_accumulated,sales_amount,source_observed_at,source_run_id
            FROM dw.fct_boxoffice_daily ORDER BY kofic_movie_cd,target_date'''))
    if not movies:
        raise ValueError('Refusing an empty service publication')
    payload = json.loads(json.dumps({
        'movies':[{key.upper():value for key,value in row.items()} for row in movies],
        'facts':[{key.upper():value for key,value in row.items()} for row in facts],
    },default=str))
    cutoff = connection.execute('SELECT clock_timestamp()').fetchone()[0]
    connection.execute('''INSERT INTO ops.service_publications
        (publication_id,cutoff,model_version,input_sha256,payload,status)
        VALUES (%s,%s,%s,%s,%s,'PENDING')''',
        (publication_id,cutoff,version,digest(payload),Jsonb(payload)))
    return publication_id


def publish_frozen(connection, service_factory, publication_id):
    with connection.cursor(row_factory=dict_row) as cursor:
        row = cursor.execute('SELECT * FROM ops.service_publications WHERE publication_id=%s', (publication_id,)).fetchone()
    if row['status']=='SUCCEEDED':
        return row['result']
    payload=row['payload']
    if digest(payload)!=row['input_sha256']:
        raise ValueError('Frozen publication checksum mismatch')
    try:
        with service_factory() as service:
            result = sync_service(service,payload['movies'],payload['facts'],cutoff=row['cutoff'],
                run_id=publication_id,model_version=row['model_version'])
        connection.execute('''UPDATE ops.service_publications SET status='SUCCEEDED',result=%s,
            error_type=NULL,finished_at=clock_timestamp() WHERE publication_id=%s''', (Jsonb(result),publication_id))
        return result
    except Exception as error:
        connection.execute('''UPDATE ops.service_publications SET status='FAILED',error_type=%s
            WHERE publication_id=%s''', (type(error).__name__,publication_id))
        raise


def build_warehouse(connection, *, orchestration_run_id, publish=False, service_factory=None, artifact_root=None):
    summary = {}
    with warehouse_lock(connection):
        publication_id = digest(['pop_talk',orchestration_run_id])
        existing = connection.execute('SELECT 1 FROM ops.service_publications WHERE publication_id=%s', (publication_id,)).fetchone()
        if publish and existing:
            summary['publication'] = publish_frozen(connection,service_factory,publication_id)
        else:
            summary.update(run_dbt(artifact_root=artifact_root))
            if publish:
                publication_id = freeze_publication(connection,orchestration_run_id,summary['model_version'])
                summary['publication'] = publish_frozen(connection,service_factory,publication_id)
    return dict(summary)
