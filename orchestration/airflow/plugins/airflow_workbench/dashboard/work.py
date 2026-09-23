"""Personal DAG work queue; writes stay in plugin tables, actions use native APIs."""

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Literal

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from airflow.models.dag import DagModel, DagTag
from airflow.models.dagrun import DagRun

from airflow_workbench.dashboard import protection
from airflow_workbench.dashboard import cache, performance
from airflow_workbench.shared import metadata
from airflow_workbench.shared.auth import access
from airflow_workbench.shared.contracts import Contract
from airflow_workbench.dashboard.database import Store, Conflict
from airflow_workbench.shared.identity import owner_id

router = APIRouter()


class Profile(Contract):
    version: int = Field(default=0, ge=0)
    dag_ids: list[str] = Field(default_factory=list, max_length=200)
    tags: list[str] = Field(default_factory=list, max_length=20)
    hours: int = Field(default=168, ge=1, le=720)
    long_running_minutes: int = Field(default=60, ge=5, le=10080)
    search: str = Field(default="", max_length=100)
    run_state: Literal["", "failed", "running", "queued", "success"] = ""
    paused: Literal["all", "active", "paused"] = "all"
    refresh_seconds: Literal[0, 30, 60, 300] = 30


def load_profile(owner):
    with Store().connection() as db:
        row = db.execute(
            "SELECT value,version FROM documents WHERE key=?",
            ("work-profile:" + owner,),
        ).fetchone()
    return (
        Profile.model_validate({**json.loads(row["value"]), "version": row["version"]})
        if row
        else Profile()
    )


def save_profile(owner, spec, allowed):
    spec = spec.model_copy(
        update={
            "dag_ids": sorted(set(spec.dag_ids)),
            "tags": sorted({t.strip() for t in spec.tags if t.strip()}),
        }
    )
    if (
        set(spec.dag_ids) - set(allowed)
        or any(len(v) > 250 for v in spec.dag_ids)
        or any(len(t) > 100 for t in spec.tags)
    ):
        raise HTTPException(422, "조회 가능한 DAG와 올바른 태그를 선택하세요.")
    with Store().connection() as db:
        key = "work-profile:" + owner
        db.lock(key)
        row = db.execute("SELECT version FROM documents WHERE key=?", (key,)).fetchone()
        if (row[0] if row else 0) != spec.version:
            raise Conflict("다른 창에서 담당 범위가 변경되었습니다. 다시 불러오세요.")
        saved = spec.model_copy(update={"version": spec.version + 1})
        db.execute(
            "INSERT INTO documents(key,value,version) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,version=EXCLUDED.version",
            (key, saved.model_dump_json(), saved.version),
        )
    return saved


@contextmanager
def read_db():
    with protection.connection(readonly=True) as db:
        yield db


def catalog(allowed, search=""):
    d, t = DagModel.__table__, DagTag.__table__
    stmt = sa.select(d.c.dag_id, d.c.dag_display_name, d.c.owners, d.c.is_paused).where(
        d.c.dag_id.in_(sorted(allowed))
    )
    if search:
        stmt = stmt.where(
            sa.or_(
                sa.func.strpos(sa.func.lower(d.c.dag_id), search.lower()) > 0,
                d.c.dag_id.in_(
                    sa.select(t.c.dag_id).where(
                        sa.func.strpos(sa.func.lower(t.c.name), search.lower()) > 0
                    )
                ),
            )
        )
    with read_db() as db:
        rows = [
            dict(r) for r in db.execute(stmt.order_by(d.c.dag_id).limit(101)).mappings()
        ]
        ids = [r["dag_id"] for r in rows[:100]]
        tags = db.execute(
            sa.select(t.c.dag_id, t.c.name).where(t.c.dag_id.in_(ids))
        ).all()
    return {
        "dags": [
            {**r, "tags": [tag for dag, tag in tags if dag == r["dag_id"]]}
            for r in rows[:100]
        ],
        "limited": len(rows) > 100,
    }


def selected(allowed, spec):
    if not spec.dag_ids and not spec.tags:
        return []
    d, t = DagModel.__table__, DagTag.__table__
    with read_db() as db:
        rows = (
            db.execute(
                sa.select(d.c.dag_id, d.c.dag_display_name, d.c.is_paused, d.c.owners)
                .where(
                    d.c.dag_id.in_(sorted(allowed)),
                    sa.or_(
                        d.c.dag_id.in_(spec.dag_ids),
                        d.c.dag_id.in_(
                            sa.select(t.c.dag_id).where(t.c.name.in_(spec.tags))
                        ),
                    ),
                )
                .order_by(d.c.dag_id)
                .limit(201)
            )
            .mappings()
            .all()
        )
        tags = db.execute(
            sa.select(t.c.dag_id, t.c.name).where(
                t.c.dag_id.in_([r["dag_id"] for r in rows])
            )
        ).all()
    if len(rows) > 200:
        raise HTTPException(
            422, "담당 DAG가 200개를 넘습니다. 업무 태그나 선택 범위를 좁혀 주세요."
        )
    return [
        {**dict(r), "tags": [tag for dag, tag in tags if dag == r["dag_id"]]}
        for r in rows
    ]


def narrow(dags, spec):
    """Search narrows the saved responsibility scope; it never expands permissions."""
    words = spec.search.casefold().split()
    return [
        d
        for d in dags
        if (spec.paused == "all" or bool(d["is_paused"]) == (spec.paused == "paused"))
        and all(
            word
            in " ".join(
                [
                    d["dag_id"],
                    d.get("dag_display_name") or "",
                    d.get("owners") or "",
                    *d.get("tags", []),
                ]
            ).casefold()
            for word in words
        )
    ]


async def workspace(user):
    spec = await protection.run(load_profile, owner_id(user))
    allowed, namespace = await protection.run(performance.scope, user)
    assigned = await protection.run(selected, allowed, spec)
    return {
        "profile": spec.model_dump(),
        "dags": narrow(assigned, spec),
        "assigned_count": len(assigned),
        "namespace": namespace,
    }


@router.get("/api/work/scope")
async def work_scope(user=Depends(access)):
    value = await workspace(user)
    return {k: v for k, v in value.items() if k != "namespace"}


def summary(dags, spec, now):
    ids = [d["dag_id"] for d in dags]
    if not ids:
        return {
            "dags": [],
            "attention": [],
            "recent": [],
            "attention_limited": False,
            "counts": {"failed": 0, "running": 0, "queued": 0, "success": 0},
            "as_of": now,
        }
    r = DagRun.__table__
    since = datetime.fromtimestamp(now - spec.hours * 3600, timezone.utc)
    source = (
        sa.select(
            r.c.dag_id,
            r.c.run_id,
            r.c.state,
            r.c.run_after,
            r.c.start_date,
            r.c.end_date,
        )
        .where(
            r.c.dag_id.in_(ids),
            sa.or_(r.c.run_after >= since, r.c.state.in_(["running", "queued"])),
        )
        .cte("work_runs")
        .prefix_with("MATERIALIZED")
    )
    queries = {
        "counts": sa.select(source.c.state, sa.func.count().label("n")).group_by(
            source.c.state
        ),
        "latest": sa.select(source)
        .distinct(source.c.dag_id)
        .order_by(source.c.dag_id, source.c.run_after.desc()),
        "successes": sa.select(
            source.c.dag_id, sa.func.max(source.c.end_date).label("ended")
        )
        .where(source.c.state == "success")
        .group_by(source.c.dag_id),
        "attention": sa.select(source)
        .where(
            sa.or_(
                source.c.state == "failed",
                sa.and_(
                    source.c.state == "running",
                    source.c.start_date
                    < datetime.fromtimestamp(
                        now - spec.long_running_minutes * 60, timezone.utc
                    ),
                ),
            )
        )
        .order_by(source.c.run_after.desc())
        .limit(101),
        "recent": sa.select(source).order_by(source.c.run_after.desc()).limit(30),
    }
    if spec.run_state:
        for name in ("counts", "latest", "attention", "recent"):
            queries[name] = queries[name].where(source.c.state == spec.run_state)
    packed = []
    for name, query in queries.items():
        rows = query.subquery(name)
        packed.append(
            sa.select(sa.func.jsonb_agg(sa.func.to_jsonb(rows.table_valued())))
            .scalar_subquery()
            .label(name)
        )
    with read_db() as db:
        result = db.execute(sa.select(*packed)).mappings().one()
    counts = {r["state"]: r["n"] for r in result["counts"] or []}
    latest = {r["dag_id"]: r for r in result["latest"] or []}
    successes = {r["dag_id"]: r["ended"] for r in result["successes"] or []}
    attention, recent = result["attention"] or [], result["recent"] or []
    # Small serializable result suitable for the shared, bounded dashboard cache.
    value = {
        "dags": [
            {
                **d,
                "latest": latest.get(d["dag_id"]),
                "last_success": successes.get(d["dag_id"]),
            }
            for d in dags
        ],
        "attention": attention[:100],
        "attention_limited": len(attention) > 100,
        "recent": recent,
        "counts": {
            s: counts.get(s, 0) for s in ("failed", "running", "queued", "success")
        },
        "as_of": now,
    }
    return json.loads(json.dumps(value, default=lambda v: v.isoformat()))


@router.get("/api/work/profile")
async def profile(user=Depends(access)):
    return await protection.run(load_profile, owner_id(user))


@router.put("/api/work/profile")
async def update_profile(spec: Profile, user=Depends(access)):
    allowed, _ = await protection.run(performance.scope, user)
    return await protection.run(save_profile, owner_id(user), spec, allowed)


@router.get("/api/work/catalog")
async def dag_catalog(q: str = "", user=Depends(access)):
    if len(q) > 100:
        raise HTTPException(422, "검색어는 100자 이내로 입력하세요.")
    allowed, _ = await protection.run(performance.scope, user)
    return await protection.run(catalog, allowed, q)


@router.get("/api/work/summary")
async def work_summary(user=Depends(access)):
    context = await workspace(user)
    spec = Profile.model_validate(context["profile"])
    dags, namespace = context["dags"], context["namespace"]
    now = int(time.time() // cache.TTL) * cache.TTL
    key = cache.digest(
        {
            "work": 4,
            "schema": Store().schema,
            "auth": namespace,
            "dags": dags,
            "hours": spec.hours,
            "long": spec.long_running_minutes,
            "state": spec.run_state,
        }
    )
    try:
        value, info = await cache.get(key, lambda: summary(dags, spec, now))
    except ValueError as exc:
        raise HTTPException(429, str(exc)) from exc
    return {
        **value,
        **info,
        "profile": spec.model_dump(),
        "assigned_count": context["assigned_count"],
    }
