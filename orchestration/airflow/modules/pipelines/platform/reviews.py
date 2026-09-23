"""Read a consistent service review snapshot without member identities or review text."""
from __future__ import annotations

import json
from psycopg.rows import dict_row

from pipelines.platform.database import VERSION, warehouse_lock
from pipelines.platform.ingest import register_load
from pipelines.transforms.movie_bronze_silver import digest

SERVICE = 'pop_talk'
MAX_REVIEWS = 100_000


def extract_snapshot(service):
    with service.transaction():
        service.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        observed = service.execute('SELECT transaction_timestamp()').fetchone()[0]
        with service.cursor(row_factory=dict_row) as cursor:
            cursor.execute('''SELECT r.id AS review_id,r.movie_id AS service_movie_id,
                m.kofic_movie_cd, CASE WHEN r.source_system IS NULL THEN 'internal:pop_talk'
                    ELSE 'external:' || r.source_system END AS source_system,
                r.source_review_key,r.rating,r.status::text,r.created_at,r.updated_at,r.deleted_at,
                CASE WHEN r.source_system IS NULL THEN r.created_at END AS original_reviewed_at
                FROM dev.reviews r LEFT JOIN dev.popcorn_movies m ON m.id=r.movie_id
                ORDER BY r.id LIMIT %s''', (MAX_REVIEWS+1,))
            rows = cursor.fetchall()
        if len(rows)>MAX_REVIEWS:
            raise ValueError('Review snapshot exceeds Phase 1 limit; implement paged extraction first')
        return observed, json.loads(json.dumps(rows,default=str))


def load_reviews(connection, service_factory, *, orchestration_run_id):
    uri = f'postgres://{SERVICE}/reviews/snapshot/{orchestration_run_id}'
    load_id = digest([uri, VERSION])
    summary = {}
    existing = connection.execute('SELECT quality FROM ops.source_loads WHERE load_id=%s', (load_id,)).fetchone()
    if existing:
        summary.update(load_id=load_id, created=False, **existing[0])
        return dict(summary)
    with service_factory() as service:
        observed, rows = extract_snapshot(service)
    quality = {'reviews':len(rows), 'snapshot_complete':True, 'service_system':SERVICE}
    with warehouse_lock(connection), connection.transaction():
        load_id, created = register_load(connection, kind='service_reviews', uri=uri,
            input_hash=digest([observed.isoformat(), rows]), source_run_id=orchestration_run_id,
            observed_at=observed, quality=quality)
        if created:
            for row in rows:
                if row['status'] not in ('ACTIVE','HIDDEN'):
                    raise ValueError('Unknown service review status')
                connection.execute('''INSERT INTO stg.review_observations VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (load_id,SERVICE,row['review_id'],row['service_movie_id'],row['kofic_movie_cd'],
                     row['source_system'],row['source_review_key'],row['rating'],row['status'],
                     row['created_at'],row['updated_at'],row['deleted_at'],row['original_reviewed_at'],observed))
    summary.update(load_id=load_id, created=created, **quality)
    return dict(summary)
