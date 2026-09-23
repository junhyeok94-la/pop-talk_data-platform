"""Bounded dashboard benchmark on an isolated 50,003-row PostgreSQL fixture."""

import asyncio
import json
import math
import statistics
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import sqlalchemy as sa

from airflow_workbench.dashboard import aggregate, performance, studio
from airflow_workbench.shared.database import Store
# Test helpers live one level above the manually invoked benchmarks.
sys.path.append(str(Path(__file__).resolve().parents[1]))
from test_workbench_performance import PerformanceTest


def timings(values):
    ordered = sorted(values)
    return {"median_ms": round(statistics.median(values), 1), "p95_ms": round(ordered[math.ceil(len(ordered) * .95) - 1], 1), "max_ms": round(max(values), 1)}


def main():
    fixture = PerformanceTest()
    fixture.setUp()
    try:
        schema = Store().schema
        with fixture.engine.begin() as db:
            db.execute(sa.text(f"INSERT INTO \"{schema}\".fixture_runs SELECT 'train','bulk-'||n,CASE WHEN n%10=0 THEN 'failed' ELSE 'success' END,to_timestamp(4000+n%3600),to_timestamp(4000+n%3600),to_timestamp(4000+n%3600+n%120) FROM generate_series(1,50000) n"))
            db.execute(sa.text(f'ANALYZE "{schema}".fixture_runs'))
        manager = Mock()
        manager.is_authorized_dag.return_value = True
        manager.get_authorized_dag_ids.return_value = {"train", "other"}
        original = aggregate.execute
        statements = []
        def track(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("WITH wb_source"):
                statements.append(statement)
        def compute(builders, context, allowed):
            return original(builders, context, allowed, table=fixture.table, engine=fixture.engine, environments={"train": ("gpu", "training")})
        board = studio.initial_boards()[0].model_copy(update={"from_ts": 1, "to_ts": 10000})
        async def roundtrip():
            started = time.perf_counter()
            result = await performance.board(board, AsyncMock(side_effect=AssertionError("Native API pagination used")), object())
            frames = [v for values in result.values() for v in values]
            assert not any("error" in f for f in frames), frames
            assert all(f["total_rows"] == 50003 and not f["truncated"] for f in frames)
            return (time.perf_counter() - started) * 1000, frames[0]["cache_hit"]
        async def run():
            cold = await asyncio.gather(*[roundtrip() for _ in range(20)])
            warm = await asyncio.gather(*[roundtrip() for _ in range(20)])
            return {
                "fixture_rows": 50003, "panels": len(board.panels), "simultaneous_viewers": 20,
                "cold_burst": timings([v[0] for v in cold]), "warm_burst": timings([v[0] for v in warm]),
                "cold_burst_reused": sum(v[1] for v in cold), "warm_burst_reused": sum(v[1] for v in warm),
            }
        sa.event.listen(fixture.engine, "before_cursor_execute", track)
        try:
            with patch("airflow_workbench.shared.auth.get_auth_manager", return_value=manager), patch.object(aggregate, "execute", side_effect=compute):
                result = asyncio.run(run())
        finally:
            sa.event.remove(fixture.engine, "before_cursor_execute", track)
        result["source_aggregate_statements"] = len(statements)
        assert len(statements) == 1, result
        result["scope"] = "Isolated PostgreSQL fixture; API function path with mocked authorization. Excludes HTTP, real auth latency, scheduler contention and prolonged load."
        print(json.dumps(result, indent=2), flush=True)
        Path("/opt/airflow/workbench/dashboard-performance-fixture.json").write_text(json.dumps(result, indent=2))
    finally:
        fixture.doCleanups()


if __name__ == "__main__":
    main()
