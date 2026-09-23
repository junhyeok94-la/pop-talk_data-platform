"""Best-effort priority for pipelines: bounded dashboard work, shed on pressure.

Uses the existing credentials with a separate, small connection pool. This is
admission control, not PostgreSQL/OS workload scheduling or physical isolation.
"""

import asyncio
import contextvars
import functools
import logging
import os
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, TimeoutError as PoolTimeout
from starlette.responses import JSONResponse

from airflow_workbench.shared import metadata
from airflow_workbench.shared.auth import authorization_runner

log = logging.getLogger(__name__)


def setting(name, default, minimum, maximum):
    value = float(os.environ.get("AIRFLOW_WORKBENCH_" + name, default))
    if not minimum <= value <= maximum:
        raise ValueError(f"AIRFLOW_WORKBENCH_{name} must be {minimum}..{maximum}")
    return value


@dataclass(frozen=True)
class Policy:
    requests: int = int(setting("DASHBOARD_REQUESTS", 4, 1, 16))
    rate: float = setting("DASHBOARD_RATE", 4, 0.1, 50)
    connections: int = int(setting("DASHBOARD_CONNECTIONS", 2, 1, 4))
    query_ms: int = int(setting("DASHBOARD_QUERY_MS", 1000, 100, 2000))
    cooldown: int = int(setting("DASHBOARD_COOLDOWN", 30, 5, 300))
    db_active: int = int(setting("DASHBOARD_DB_ACTIVE", 8, 1, 1000))
    db_connections: float = setting("DASHBOARD_DB_CONNECTION_RATIO", 0.75, 0.1, 0.95)
    probe_ms: int = int(setting("DASHBOARD_PROBE_MS", 200, 20, 1000))
    lag_ms: int = int(setting("DASHBOARD_LOOP_LAG_MS", 100, 10, 1000))
    stale_seconds: int = int(setting("DASHBOARD_STALE_SECONDS", 300, 30, 1800))
    compute_gap: float = setting("DASHBOARD_COMPUTE_GAP", 2, 0, 60)


policy = Policy()


class Deferred(Exception):
    def __init__(self, reason="capacity", retry_after=None):
        self.reason = reason
        self.retry_after = retry_after or policy.cooldown
        super().__init__("파이프라인 처리를 우선하여 대시보드 갱신을 잠시 미룹니다.")

    def detail(self):
        return {
            "message": str(self),
            "reason": self.reason,
            "retry_after": self.retry_after,
        }


class Guard:
    def __init__(self):
        self.lock = threading.RLock()
        self.reset()

    def reset(self):
        with self.lock:
            self.active = 0
            self.tokens = policy.requests * 2.0
            self.updated = time.monotonic()
            self.until = 0.0
            self.reason = None
            self.probe_due = 0.0
            self.healthy = 0
            self.rejected = 0

    def trip(self, reason):
        with self.lock:
            if self.reason != reason:
                log.warning("Dashboard refresh deferred: %s", reason)
            self.until = time.monotonic() + policy.cooldown
            self.reason = reason
            self.healthy = 0

    def blocked(self):
        with self.lock:
            return self.reason if time.monotonic() < self.until else None

    def enter(self):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(
                policy.requests * 2, self.tokens + (now - self.updated) * policy.rate
            )
            self.updated = now
            reason = self.blocked()
            if reason or self.active >= policy.requests or self.tokens < 1:
                self.rejected += 1
                raise Deferred(reason or "request_limit")
            self.tokens -= 1
            self.active += 1

    def leave(self):
        with self.lock:
            self.active -= 1


guard = Guard()
_engine = None
_engine_key = None
_engine_lock = threading.Lock()
_probe_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="workbench-dashboard")
_jobs = threading.BoundedSemaphore(4)
_monitored = weakref.WeakSet()


def engine():
    global _engine, _engine_key
    original = metadata.engine()
    key = (os.getpid(), id(original), policy.connections)
    with _engine_lock:
        if _engine_key != key:
            if _engine is not None:
                _engine.dispose()
            from airflow import settings
            from airflow.configuration import conf

            options = (
                dict(conf.getimport("database", "sql_alchemy_connect_args"))
                if conf.has_option("database", "sql_alchemy_connect_args")
                else {}
            )
            options.update(connect_timeout=2, application_name="workbench_dashboard")
            # Preserve Airflow's SSL and custom token-auth engine factory. Only the
            # dashboard pool/transactions receive the conservative limits.
            _engine = settings.create_metadata_engine(
                original.url.render_as_string(hide_password=False),
                engine_args={
                    "pool_size": policy.connections,
                    "max_overflow": 0,
                    "pool_timeout": 0.05,
                    "pool_recycle": 300,
                    "pool_pre_ping": True,
                    "execution_options": dict(original.get_execution_options()),
                },
                connect_args=options,
            )
            _engine_key = key
        return _engine


@contextmanager
def connection(*, readonly=False, database_engine=None):
    try:
        with (
            database_engine if database_engine is not None else engine()
        ).begin() as db:
            db.execute(sa.text(f"SET LOCAL statement_timeout = '{policy.query_ms}ms'"))
            db.execute(sa.text("SET LOCAL lock_timeout = '100ms'"))
            db.execute(
                sa.text("SET LOCAL idle_in_transaction_session_timeout = '2500ms'")
            )
            db.execute(sa.text("SET LOCAL max_parallel_workers_per_gather = 0"))
            db.execute(sa.text("SET LOCAL work_mem = '4MB'"))
            if readonly:
                db.execute(sa.text("SET LOCAL transaction_read_only = on"))
            yield db
    except (PoolTimeout, DBAPIError) as exc:
        code = getattr(getattr(exc, "orig", None), "pgcode", None)
        if (
            isinstance(exc, DBAPIError)
            and not exc.connection_invalidated
            and code not in {None, "57014", "55P03", "53300", "57P01", "57P02", "57P03"}
            and not str(code).startswith("08")
        ):
            raise
        guard.trip("database_wait")
        raise Deferred("database_wait") from exc


@contextmanager
def store_connection(schema):
    with connection() as db:
        db.execute(
            sa.text(f'SET LOCAL search_path TO "{metadata.schema_name(schema)}"')
        )
        yield metadata.Database(db, schema)


async def run(fn, *args, **kwargs):
    # A cancelled HTTP request does not release its slot while its thread runs.
    if not _jobs.acquire(blocking=False):
        raise Deferred("worker_limit")
    ctx = contextvars.copy_context()
    try:
        future = _executor.submit(ctx.run, functools.partial(fn, *args, **kwargs))
    except BaseException:
        _jobs.release()
        raise
    future.add_done_callback(lambda _: _jobs.release())
    return await asyncio.wrap_future(future)


def database_pressure(db):
    """Inspect only server activity counters, never query text or credentials.

    At most one sample per five seconds per process. Failure is fail-closed.
    Activity is a pressure signal, not a measurement of remote DB CPU or I/O.
    """
    with guard.lock:
        reason = guard.blocked()
        if reason:
            return reason
        if _probe_lock.locked():
            return guard.reason or "database_probe"
        now = time.monotonic()
        if now < guard.probe_due:
            return guard.reason
    # Never hold Guard.lock across database I/O: enter() runs on the API loop.
    if not _probe_lock.acquire(blocking=False):
        return "database_probe"
    try:
        with guard.lock:
            if time.monotonic() < guard.probe_due:
                return guard.reason
            guard.probe_due = now + 5
        started = time.monotonic()
        row = (
            db.execute(
                sa.text(
                    """
            SELECT count(*) FILTER (WHERE backend_type='client backend') AS connections,
              count(*) FILTER (WHERE datname=current_database() AND state='active'
                AND coalesce(application_name,'') NOT LIKE 'workbench_dashboard%'
                AND pid<>pg_backend_pid()) AS active,
              count(*) FILTER (WHERE datname=current_database() AND wait_event_type='Lock'
                AND coalesce(application_name,'') NOT LIKE 'workbench_dashboard%') AS waiting,
              current_setting('max_connections')::int AS maximum
            FROM pg_stat_activity
        """
                )
            )
            .mappings()
            .one()
        )
        elapsed_ms = (time.monotonic() - started) * 1000
        reason = (
            "database_connections"
            if row["connections"] >= row["maximum"] * policy.db_connections
            else (
                "database_active"
                if row["active"] >= policy.db_active
                else (
                    "database_locks"
                    if row["waiting"] >= 1
                    else "database_latency" if elapsed_ms >= policy.probe_ms else None
                )
            )
        )
        with guard.lock:
            if reason:
                guard.trip(reason)
            elif guard.reason:
                guard.healthy += 1
                if guard.healthy >= 2:
                    log.info("Dashboard refresh resumed after healthy database samples")
                    guard.reason = None
            return reason or guard.reason
    finally:
        _probe_lock.release()


def check_pressure():
    reason = guard.blocked()
    if not reason:
        with connection(readonly=True) as db:
            reason = database_pressure(db)
    if reason:
        raise Deferred(reason)


async def _watch_loop():
    while True:
        started = time.monotonic()
        await asyncio.sleep(0.25)
        if (time.monotonic() - started - 0.25) * 1000 > policy.lag_ms:
            guard.trip("api_latency")


def dashboard_request(path):
    route = path.split("/api/", 1)[-1] if "/api/" in path else ""
    return route.startswith(
        ("studio/", "boards", "datasources", "dashboard")
    ) or route in {
        "query",
        "work/profile",
        "work/scope",
        "work/catalog",
        "work/summary",
    }


async def deferred_handler(request, exc):
    return JSONResponse(
        {"detail": exc.detail()},
        status_code=503,
        headers={"Retry-After": str(exc.retry_after), "Cache-Control": "no-store"},
    )


class Middleware:
    """Reject excess dashboard traffic before plugin auth/body processing.

    Native Airflow routes, explicit DAG actions, Model Lab and executor callbacks
    never enter this gate. Frontend keeps previously rendered data on deferral.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not dashboard_request(scope.get("path", "")):
            return await self.app(scope, receive, send)
        loop = asyncio.get_running_loop()
        if loop not in _monitored:
            _monitored.add(loop)
            loop.create_task(_watch_loop())
        try:
            guard.enter()
        except Deferred as exc:
            return await (await deferred_handler(None, exc))(scope, receive, send)
        token = metadata.connection_override.set(store_connection)
        auth_token = authorization_runner.set(run)
        try:
            await self.app(scope, receive, send)
        finally:
            authorization_runner.reset(auth_token)
            metadata.connection_override.reset(token)
            guard.leave()
