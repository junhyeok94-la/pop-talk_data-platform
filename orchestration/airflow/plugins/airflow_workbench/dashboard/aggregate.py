"""Bounded PostgreSQL aggregation over a narrow, permission-filtered DAG projection.

Only generated SQLAlchemy expressions are executed here, never user-entered SQL.
The shared source CTE is materialized once for all panels in a request.
"""

import json
import time
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from airflow_workbench.dashboard import studio
from airflow_workbench.dashboard import protection
from airflow_workbench.dashboard.mlops_source import environment_index
from airflow_workbench.shared import metadata

NUMERIC = {"run_ts", "start_ts", "end_ts", "duration_seconds"}


def identity(builder):
    return json.dumps(builder.model_dump(), sort_keys=True, separators=(",", ":"))


def projection(table, context, allowed, environments):
    """Accepting a Table also lets tests use an isolated fixture, not Airflow rows."""
    since, until = context.bounds()
    c = table.c
    duration = sa.extract("epoch", c.end_date - c.start_date)

    def mapping(position):
        return (
            sa.case(
                *[
                    (c.dag_id == dag, values[position])
                    for dag, values in environments.items()
                ],
                else_=None
            )
            if environments
            else sa.cast(sa.null(), sa.Text)
        )

    return (
        sa.select(
            c.dag_id,
            c.run_id,
            c.state,
            sa.extract("epoch", c.run_after).cast(sa.Float).label("run_ts"),
            sa.extract("epoch", c.start_date).cast(sa.Float).label("start_ts"),
            sa.extract("epoch", c.end_date).cast(sa.Float).label("end_ts"),
            sa.case((duration >= 0, duration.cast(sa.Float)), else_=None).label(
                "duration_seconds"
            ),
            mapping(0).label("environment_id"),
            mapping(1).label("workload"),
        )
        .where(
            c.dag_id.in_(sorted(allowed)),
            c.state == context.run_state if context.run_state else sa.true(),
            c.run_after >= datetime.fromtimestamp(since, timezone.utc),
            c.run_after <= datetime.fromtimestamp(until, timezone.utc),
        )
        .cte("wb_source")
        .prefix_with("MATERIALIZED")
    )


def query(source, builder, context):
    # Keep field/alias/variable validation identical to the API builder.
    studio.build_result(builder, {"dag_runs": []}, context)
    if builder.dataset != "dag_runs":
        raise ValueError("Airflow 데이터 소스에는 dag_runs를 사용하세요.")
    fields = {name: source.c[name] for name in studio.SCHEMAS["dag_runs"]}
    predicates = []
    for f in builder.filters:
        col = fields[f.field]
        string = sa.cast(col, sa.Text)
        value = context.variables[f.value[1:]] if f.value.startswith(":") else f.value
        if f.op == "not_null":
            condition = col.is_not(None)
        elif f.op == "contains":
            condition = (
                sa.func.strpos(
                    sa.func.lower(sa.func.coalesce(string, "")), value.lower()
                )
                > 0
            )
        elif f.op == "in":
            condition = sa.func.coalesce(string, "None").in_(
                [v.strip() for v in value.split(",")]
            )
        elif f.op in ("eq", "ne"):
            string = sa.func.coalesce(string, "None")
            condition = string == value if f.op == "eq" else string != value
        else:
            try:
                number = float(value)
            except ValueError:
                condition = sa.false()
            else:
                numeric = (
                    col
                    if f.field in NUMERIC
                    else sa.case(
                        (
                            string.op("~")(
                                r"^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$"
                            ),
                            sa.cast(string, sa.Float),
                        ),
                        else_=None,
                    )
                )
                condition = numeric >= number if f.op == "gte" else numeric <= number
        predicates.append(condition)
    if builder.bucket:
        if builder.bucket not in NUMERIC:
            raise ValueError("시간 버킷에는 숫자 시간 필드를 사용하세요.")
        epoch = (
            sa.func.floor(fields[builder.bucket] / context.bucket_seconds)
            * context.bucket_seconds
        )
        fields[builder.bucket] = sa.func.to_char(
            sa.func.timezone("UTC", sa.func.to_timestamp(epoch)),
            'YYYY-MM-DD"T"HH24:MI:SS"+00:00"',
        )
    grouped = bool(builder.group_by or builder.measures)
    values = {k: fields[k] for k in builder.group_by} if grouped else fields.copy()
    for m in builder.measures:
        col = fields.get(m.field)
        if m.op == "count":
            expr = sa.func.count()
        elif m.op == "percent":
            expr = (
                100.0
                * sa.func.count().filter(sa.cast(col, sa.Text) == m.match)
                / sa.func.nullif(sa.func.count(col), 0)
            )
        elif m.field not in NUMERIC or m.field == builder.bucket:
            expr = sa.cast(sa.null(), sa.Float)
        elif m.op == "p95":
            expr = sa.func.percentile_disc(0.95).within_group(col)
        else:
            expr = getattr(sa.func, m.op)(col)
        values[m.alias] = expr
    columns = builder.columns or list(values)
    stmt = (
        sa.select(*[values[k].label(k) for k in columns])
        .select_from(source)
        .where(*predicates)
    )
    if builder.group_by:
        stmt = stmt.group_by(*[fields[k] for k in builder.group_by])
    if builder.sort:
        order = values[builder.sort]
        stmt = stmt.order_by(
            (order.desc() if builder.descending else order.asc()).nulls_last()
        )
    return stmt.limit(builder.limit + 1), columns


def statement(source, builders, context):
    parts, columns = [], {}
    for i, (key, builder) in enumerate(builders.items()):
        stmt, columns[key] = query(source, builder, context)
        rows = stmt.subquery("panel_" + str(i))
        payload = sa.select(
            sa.func.jsonb_agg(sa.func.to_jsonb(rows.table_valued()))
        ).scalar_subquery()
        parts.append(
            sa.select(
                sa.literal(key).label("key"),
                sa.select(sa.func.count())
                .select_from(source)
                .scalar_subquery()
                .label("source_rows"),
                payload.label("rows"),
            )
        )
    return sa.union_all(*parts), columns


def execute(builders, context, allowed, *, table=None, engine=None, environments=None):
    if table is None:
        from airflow.models.dagrun import DagRun

        table = DagRun.__table__
    environments = environment_index() if environments is None else environments
    started = time.monotonic()
    source = projection(table, context, allowed, environments)
    stmt, columns = statement(source, builders, context)
    try:
        with protection.connection(readonly=True, database_engine=engine) as db:
            rows = db.execute(stmt).mappings().all()
    except DBAPIError as exc:
        raise ValueError(
            "집계가 제한 시간 내 완료되지 않았습니다. 조회 기간이나 패널 수를 줄여 주세요."
        ) from exc
    elapsed = round((time.monotonic() - started) * 1000, 1)
    since, until = context.bounds()
    result = {}
    for item in rows:
        key = item["key"]
        output = item["rows"] or []
        limit = builders[key].limit
        result[key] = {
            "columns": columns[key],
            "rows": output[:limit],
            "limited": len(output) > limit,
            "source_rows": item["source_rows"],
            "total_rows": item["source_rows"],
            "truncated": False,
            "from_ts": since,
            "to_ts": until,
            "source": "postgresql-aggregate",
            "aggregate_ms": elapsed,
        }
    if len(json.dumps(result).encode()) > 4 * 1024 * 1024:
        raise ValueError(
            "보드 집계 결과가 4MB를 초과합니다. 표시 행이나 패널 수를 줄여 주세요."
        )
    return result
