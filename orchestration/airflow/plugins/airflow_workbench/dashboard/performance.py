"""Authorized native aggregation and short result reuse for dashboard requests."""

import time

from fastapi import HTTPException
from airflow.api_fastapi.auth.managers.models.resource_details import (
    DagAccessEntity,
    DagDetails,
)

from airflow_workbench.dashboard import protection
from airflow_workbench.dashboard import aggregate, cache, studio
from airflow_workbench.shared import auth
from airflow_workbench.dashboard.database import Store


def scope(user):
    manager = auth.get_auth_manager()
    if not manager.is_authorized_dag(
        method="GET",
        access_entity=DagAccessEntity.RUN,
        details=DagDetails(id=None),
        user=user,
    ):
        raise HTTPException(403, "DAG 실행 조회 권한이 없습니다.")
    allowed = manager.get_authorized_dag_ids(user=user, method="GET")
    manager_type = type(manager)
    return allowed, manager_type.__module__ + "." + manager_type.__qualname__


async def board(spec, fetch, user, *, allow_postgres=False, _uncached=False):
    if (
        not _uncached
        and not spec.follow_work_scope
        and any(
            q.enabled and q.datasource != "airflow"
            for p in spec.panels
            for q in p.queries
        )
    ):
        # Independent SQL/experiment boards consume the same global budget. No
        # alternate datasource can bypass admission or keep a request alive 25s.
        from airflow_workbench.shared.identity import owner_id
        from airflow_workbench.dashboard import postgres

        allowed, namespace = await protection.run(scope, user)
        sources = await protection.run(postgres.configured)
        key = cache.digest(
            {
                "independent": 1,
                "schema": Store().schema,
                "owner": owner_id(user),
                "auth": namespace,
                "dags": sorted(allowed),
                "sql_allowed": allow_postgres,
                "sources": [s.model_dump() for s in sources],
                "board": spec.model_dump(),
            }
        )
        result, info = await cache.get(
            key,
            lambda: board(
                spec, fetch, user, allow_postgres=allow_postgres, _uncached=True
            ),
            asynchronous=True,
        )
        return {
            panel: [{**frame, **info} for frame in frames]
            for panel, frames in result.items()
        }
    started = time.monotonic()
    relative = spec.from_ts is None or spec.follow_work_scope
    workspace = None
    if spec.follow_work_scope:
        from airflow_workbench.dashboard import work

        workspace = await work.workspace(user)
        profile = workspace["profile"]
        spec = spec.model_copy(
            update={
                "hours": profile["hours"],
                "from_ts": None,
                "to_ts": None,
                "run_state": profile["run_state"],
            }
        )
    # Stable buckets let simultaneous viewers reuse results; the UI shows data time.
    if spec.from_ts is None:
        until = int(time.time() // cache.TTL) * cache.TTL
        spec = spec.model_copy(
            update={"from_ts": until - spec.hours * 3600, "to_ts": until}
        )
    builders, errors = {}, {}
    for panel in spec.panels:
        for q in panel.queries:
            if not q.enabled or q.datasource != "airflow":
                continue
            key = aggregate.identity(q.builder)
            try:
                studio.build_result(q.builder, {"dag_runs": []}, spec)
                builders[key] = q.builder
            except ValueError as exc:
                errors[key] = {"error": str(exc), "columns": [], "rows": []}
    resolved = dict(errors)
    if builders:
        # Always re-evaluate permissions, including on cache hits. Identical current
        # DAG scopes may share results; identities/tokens never come from JSON input.
        if workspace is not None:
            allowed = {d["dag_id"] for d in workspace["dags"]}
            namespace = workspace["namespace"]
        else:
            allowed, namespace = await protection.run(scope, user)
        key = cache.digest(
            {
                "version": 3,
                "schema": Store().schema,
                "auth": namespace,
                "dags": sorted(allowed),
                "queries": sorted(builders),
                "bounds": {"hours": spec.hours} if relative else spec.bounds(),
                "bucket": spec.bucket_seconds,
                "variables": spec.variables,
                "state": spec.run_state,
            }
        )
        try:
            if _uncached:
                results = await protection.run(
                    aggregate.execute, builders, spec, allowed
                )
                info = {"cache_hit": False, "sampled_at": time.time()}
            else:
                results, info = await cache.get(
                    key, lambda: aggregate.execute(builders, spec, allowed)
                )
            resolved.update(
                {
                    k: {
                        **v,
                        **info,
                        "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                    }
                    for k, v in results.items()
                }
            )
        except ValueError as exc:
            resolved.update(
                {k: {"error": str(exc), "columns": [], "rows": []} for k in builders}
            )
    result = await studio.run_board(
        spec,
        fetch,
        allow_postgres=allow_postgres,
        resolved=resolved,
        assigned_scope=workspace is not None,
    )
    if workspace is not None:
        for frames in result.values():
            for frame in frames:
                frame["work_scope"] = {
                    "version": profile["version"],
                    "dag_count": len(allowed) if builders else len(workspace["dags"]),
                    "hours": profile["hours"],
                    "run_state": profile["run_state"],
                }
    return result
