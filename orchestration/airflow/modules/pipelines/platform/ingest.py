"""Validate immutable Raw documents and atomically register a load with its STG rows."""
from __future__ import annotations

from datetime import datetime
import copy
from psycopg.types.json import Jsonb

from pipelines.platform.database import VERSION, warehouse_lock
from pipelines.transforms.movie_bronze_silver import digest, transform_run, transform_legacy_snapshot


def timestamp(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Source observation time must have a timezone')
    return result


def read_bundle(store, ready_input):
    def read(key):
        value = store.get(key)
        if not isinstance(value, dict):
            raise ValueError('Missing or invalid Raw document')
        return value
    if ready_input.get('kind') == 'initial':
        success = read(ready_input['ready_manifest_key'])
        if success.get('run_id') != ready_input['raw_run_id']:
            raise ValueError('Initial SUCCESS body does not match manifest identity')
        stages = {name: read(key) for name,key in success['stage_manifests'].items()}
        raw = {ref['key']: read(ref['key']) for stage in stages.values() for ref in stage['objects']}
        return initial_bundle(success, stages, raw)
    ready = read(ready_input['ready_manifest_key'])
    if ready.get('run_id') != ready_input['raw_run_id']:
        raise ValueError('READY body does not match event run identity')
    if ready.get('scope', {}).get('collection_date') != ready_input['collection_date']:
        raise ValueError('READY body does not match event collection date')
    success = read(ready['raw_success_manifest'])
    stages = {name: read(key) for name,key in success['stage_manifests'].items()}
    raw = {ref['key']: read(ref['key']) for stage in stages.values() for ref in stage['objects']}
    return ready, success, stages, raw


def initial_bundle(success, stages, raw):
    """Adapt the initial collector's explicit completeness contract; never accept samples."""
    scope = success['scope']
    listing = stages['movie_list']
    years = listing.get('years', [])
    if (scope.get('max_movies_per_year') != 0 or success.get('catalog_complete') is not True
            or listing.get('complete') is not True
            or {row['year'] for row in years} != set(range(scope['start_year'],scope['end_year']+1))
            or any(row.get('complete') is not True or row['selected'] != row['reported_total'] for row in years)):
        raise ValueError('Initial catalogue is sampled or incomplete')
    normalized=copy.deepcopy(stages)
    normalized['movie_list']['candidate_scope_complete']=True
    ready={'schema_version':success['schema_version'],'run_id':success['run_id'],
           'scope':scope,'stage':'DAILY_READY','status':success['status'],
           'sample':False,'candidate_scope_complete':True,'movie_count':success['movie_count']}
    return ready,success,normalized,raw


def register_load(connection, *, kind, uri, input_hash, source_run_id, observed_at, quality):
    load_id = digest([uri, VERSION])
    existing = connection.execute('SELECT input_sha256 FROM ops.source_loads WHERE load_id=%s', (load_id,)).fetchone()
    if existing:
        if existing[0] != input_hash:
            raise ValueError('Immutable source changed for the same load identity')
        return load_id, False
    connection.execute('''INSERT INTO ops.source_loads
        (load_id,source_kind,source_uri,loader_version,input_sha256,source_run_id,observed_at,quality)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
        (load_id,kind,uri,VERSION,input_hash,source_run_id,observed_at,Jsonb(quality)))
    return load_id, True


def load_legacy_snapshot(connection, manifest, body, *, uri):
    transformed = transform_legacy_snapshot(manifest, body)
    rows = transformed['silver_movies']
    if not rows or any(not row['kofic_movie_cd'] for row in rows):
        raise ValueError('Initial snapshot needs nonempty, identifiable movie records')
    with warehouse_lock(connection), connection.transaction():
        load_id, created = register_load(connection, kind='legacy_snapshot', uri=uri,
            input_hash=digest([manifest,digest(body)]), source_run_id=transformed['quality']['source_run_id'],
            observed_at=None, quality=transformed['quality'])
        if created:
            with connection.cursor() as cursor, cursor.copy('COPY stg.movie_observations FROM STDIN') as output:
                for row in rows:
                    output.write_row((load_id,row['kofic_movie_cd'],manifest['key'],None,
                                      row['content_sha256'],Jsonb(row)))
    return dict(load_id=load_id,created=created,movies=len(rows),boxoffice=0)


def load_movie_bundle(connection, bundle, *, uri, orchestration_run_id):
    summary = {}
    ready, success, stages, raw = bundle
    transformed = transform_run(ready, success, stages, raw)
    observed = {key: timestamp(value['collected_at']) for key,value in raw.items()}
    with warehouse_lock(connection), connection.transaction():
        kind = 'movie_initial' if 'start_year' in ready['scope'] else 'movie_daily'
        load_id, created = register_load(connection, kind=kind, uri=uri,
            input_hash=digest(bundle), source_run_id=ready['run_id'],
            observed_at=max(observed.values(), default=None), quality=transformed['quality'])
        if created:
            for row in transformed['silver_movies']:
                connection.execute('''INSERT INTO stg.movie_observations VALUES (%s,%s,%s,%s,%s,%s)''',
                    (load_id,row['kofic_movie_cd'],row['source_object_key'],observed[row['source_object_key']],
                     row['content_sha256'],Jsonb(row)))
            for row in transformed['silver_boxoffice']:
                connection.execute('''INSERT INTO stg.boxoffice_observations VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (load_id,row['kofic_movie_cd'],row['target_date'],row['rank'],row['audience_count'],
                     row['audience_accumulated'],row['sales_amount'],row['source_object_key'],
                     observed[row['source_object_key']],row['observation_sha256']))
    summary.update(load_id=load_id, created=created, movies=len(transformed['silver_movies']),
                   boxoffice=len(transformed['silver_boxoffice']))
    return dict(summary)
