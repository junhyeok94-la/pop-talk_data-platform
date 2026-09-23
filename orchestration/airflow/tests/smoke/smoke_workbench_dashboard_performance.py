"""Authenticated HTTP burst against the saved dashboard; no configuration writes."""

import asyncio
import json
import math
import statistics
import time
from pathlib import Path

import httpx
from prepare_model_lab_workflow import client


def main():
    authenticated = client()
    try:
        response = authenticated.get("/workbench/api/studio/boards")
        response.raise_for_status()
        board = next(b for b in response.json() if b["panels"] and all(q["datasource"] == "airflow" for p in b["panels"] for q in p["queries"]))
        # This unique exact interval makes a cold key without deleting production caches.
        now = time.time()
        board.update(from_ts=now - 24 * 3600, to_ts=now)
        headers = dict(authenticated.headers)
    finally:
        authenticated.close()
    async def run():
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8080", headers=headers, timeout=30, trust_env=False) as c:
            async def request():
                start = time.perf_counter()
                response = await c.post("/workbench/api/studio/boards/query", json=board)
                response.raise_for_status()
                frames = [f for group in response.json().values() for f in group]
                assert frames and all(f.get("source") == "postgresql-aggregate" and not f.get("error") for f in frames), frames
                assert all(not f["truncated"] for f in frames)
                return (time.perf_counter() - start) * 1000, frames[0]["cache_hit"], frames[0]["source_rows"]
            result = {"simultaneous_requests": 20, "panels": len(board["panels"])}
            for name in ("cold_burst", "warm_burst"):
                values = await asyncio.gather(*[request() for _ in range(20)])
                ms = sorted(v[0] for v in values)
                result[name] = {"median_ms": round(statistics.median(ms), 1), "p95_ms": round(ms[math.ceil(len(ms) * .95) - 1], 1), "reused": sum(v[1] for v in values), "source_rows": values[0][2]}
            return result
    result = asyncio.run(run())
    result["scope"] = "Current local Airflow HTTP API, real authentication, 20 parallel requests with one account; no browser rendering or sustained scheduler-load benchmark."
    print(json.dumps(result, indent=2))
    Path("/opt/airflow/workbench/dashboard-performance-http.json").write_text(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
