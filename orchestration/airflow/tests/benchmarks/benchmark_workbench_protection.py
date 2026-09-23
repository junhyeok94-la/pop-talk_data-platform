"""Bounded coexistence check in a disposable schema, with production guard defaults.

HTTP is ASGI in process, without real authentication. The native-route transaction
is a heartbeat surrogate, not an Airflow scheduler SLO or a production load test.
"""

import asyncio
import json
import math
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import sqlalchemy as sa
from fastapi import FastAPI

from airflow_workbench.dashboard import cache, protection
from airflow_workbench.dashboard.database import Store
from airflow_workbench.shared import metadata
sys.path.append(str(Path(__file__).resolve().parents[1]))
from workbench_test_db import isolated_database


def stats(values):
    values = sorted(values)
    return {
        "count": len(values),
        "p95_ms": round(values[math.ceil(len(values) * 0.95) - 1], 2),
        "max_ms": round(max(values), 2),
    }


def main():
    defaults = protection.policy
    fixture = unittest.TestCase()
    isolated_database(fixture)
    schema = Store().schema
    try:
        # isolated_database relaxes limits for functional fixtures; this benchmark
        # explicitly restores the actual deployment policy.
        with patch.object(protection, "policy", defaults):
            protection.guard.reset()
            native = metadata.engine()
            with native.begin() as db:
                db.execute(sa.text(f'CREATE TABLE "{schema}".heartbeat (tick bigint)'))
                db.execute(sa.text(f'INSERT INTO "{schema}".heartbeat VALUES (0)'))
                db.execute(
                    sa.text(
                        f'CREATE TABLE "{schema}".runs AS SELECT n, n%100 AS owner, n%4 AS state FROM generate_series(1,50000) n'
                    )
                )
                db.execute(sa.text(f'ANALYZE "{schema}".runs'))
            count, active, peak = 0, 0, 0
            lock = threading.Lock()

            def compute(key):
                nonlocal count, active, peak
                with lock:
                    count += 1
                    active += 1
                    peak = max(peak, active)
                try:
                    with protection.connection(readonly=True) as db:
                        return [
                            dict(row)
                            for row in db.execute(
                                sa.text(
                                    f'SELECT state,count(*) FROM "{schema}".runs WHERE owner=:owner GROUP BY state'
                                ),
                                {"owner": key},
                            ).mappings()
                        ]
                finally:
                    with lock:
                        active -= 1

            def heartbeat():
                with native.begin() as db:
                    return db.execute(
                        sa.text(
                            f'UPDATE "{schema}".heartbeat SET tick=tick+1 RETURNING tick'
                        )
                    ).scalar_one()

            app = FastAPI()
            app.add_middleware(protection.Middleware)
            app.add_exception_handler(protection.Deferred, protection.deferred_handler)

            @app.get("/api/work/summary")
            async def summary(key: int):
                value, info = await cache.get(str(key), lambda: compute(key))
                return {"rows": value, **info}

            @app.post("/execution/heartbeat")
            async def beat():
                return {"tick": await asyncio.to_thread(heartbeat)}

            async def measure():
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="http://test"
                ) as client:

                    async def beats(total):
                        latencies = []
                        for _ in range(total):
                            started = time.perf_counter()
                            response = await client.post("/execution/heartbeat")
                            assert response.status_code == 200, response.text
                            latencies.append((time.perf_counter() - started) * 1000)
                            await asyncio.sleep(0.025)
                        return stats(latencies)

                    async def dashboard_bursts():
                        statuses, latencies = {}, []

                        async def one(key):
                            started = time.perf_counter()
                            response = await client.get(
                                "/api/work/summary", params={"key": key}
                            )
                            assert response.status_code in (200, 503), response.text
                            if response.status_code == 503:
                                assert int(response.headers["retry-after"]) > 0
                            latencies.append((time.perf_counter() - started) * 1000)
                            statuses[str(response.status_code)] = (
                                statuses.get(str(response.status_code), 0) + 1
                            )

                        for _ in range(20):
                            await asyncio.gather(*(one(i) for i in range(50)))
                            await asyncio.sleep(0.25)
                        return {"status_counts": statuses, **stats(latencies)}

                    baseline = await beats(80)
                    native_under_load, dashboards = await asyncio.gather(
                        beats(240), dashboard_bursts()
                    )
                    return {
                        "native_baseline": baseline,
                        "native_with_dashboard_bursts": native_under_load,
                        "dashboard_requests": dashboards,
                    }

            result = asyncio.run(measure())
            result.update(
                fixture_rows=50000,
                distinct_dashboard_conditions=50,
                aggregate_executions=count,
                peak_aggregates=peak,
                dashboard_pool_size=protection.engine().pool.size(),
                scope="In-process ASGI, disposable PostgreSQL schema, no authentication. Native route performs a small committed write. Not a real scheduler/pipeline SLO benchmark.",
            )
            assert peak <= 1, result
            assert (
                result["dashboard_requests"]["status_counts"].get("503", 0) > 900
            ), result
            print(json.dumps(result, indent=2))
            Path("/opt/airflow/workbench/dashboard-protection-fixture.json").write_text(
                json.dumps(result, indent=2)
            )
    finally:
        fixture.doCleanups()


if __name__ == "__main__":
    main()
