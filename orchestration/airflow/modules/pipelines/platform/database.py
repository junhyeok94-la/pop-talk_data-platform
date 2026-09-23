"""Runtime credentials never use the platform administrator or Airflow metadata role."""
from __future__ import annotations

from contextlib import contextmanager
import os

import psycopg

VERSION = 'phase1-v1'
LOCK_NAME = 'pop-talk-phase1-warehouse'


def connect():
    return psycopg.connect(
        host=os.environ['POP_TALK_PLATFORM_POSTGRES_HOST'],
        port=int(os.environ.get('POP_TALK_PLATFORM_POSTGRES_PORT', '5432')),
        dbname=os.environ['POP_TALK_PLATFORM_POSTGRES_DB'],
        user=os.environ['POP_TALK_PLATFORM_POSTGRES_USER'],
        password=os.environ['POP_TALK_PLATFORM_POSTGRES_PASSWORD'],
        autocommit=True, connect_timeout=10,
        application_name='pop-talk-phase1',
    )


def service_connect(*, readonly=False):
    prefix = 'POP_TALK_REVIEW_POSTGRES_' if readonly else 'POP_TALK_POSTGRES_'
    # A read-only service account is configured separately from the publication account.
    return psycopg.connect(
        host=os.environ[prefix+'HOST'], port=int(os.environ.get(prefix+'PORT', '5432')),
        dbname=os.environ[prefix+'DB'], user=os.environ[prefix+'USER'],
        password=os.environ[prefix+'PASSWORD'], connect_timeout=10, autocommit=True,
        options='-c default_transaction_read_only=on' if readonly else '',
        application_name='pop-talk-phase1-reviews' if readonly else 'pop-talk-phase1-publish',
    )


@contextmanager
def warehouse_lock(connection):
    # Fail promptly so Airflow can retry; do not wait indefinitely behind a dbt build.
    if not connection.execute('SELECT pg_try_advisory_lock(hashtext(%s))', (LOCK_NAME,)).fetchone()[0]:
        raise RuntimeError('Another Phase 1 load/build is running; retry later')
    try:
        yield
    finally:
        connection.execute('SELECT pg_advisory_unlock(hashtext(%s))', (LOCK_NAME,))

