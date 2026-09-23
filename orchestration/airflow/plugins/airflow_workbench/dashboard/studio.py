"""Dashboard contracts, builder semantics and non-native data-source adapters."""

import asyncio
import json
import math
import statistics
import time
import uuid
from datetime import datetime, timezone
from typing import Literal

from pydantic import Field, model_validator

from airflow_workbench.dashboard import protection
from airflow_workbench.dashboard import postgres
from airflow_workbench.shared.contracts import Contract, ShortName
from airflow_workbench.dashboard.database import Conflict, NotFound, Store

SCHEMAS = {
    "dag_runs": [
        "dag_id",
        "run_id",
        "state",
        "run_ts",
        "start_ts",
        "end_ts",
        "duration_seconds",
        "environment_id",
        "workload",
    ],
    "experiments": ["id", "name", "kind", "status", "created", "finished"],
    "model_results": [
        "experiment_id",
        "created",
        "model",
        "latency_seconds",
        "tokens_per_second",
        "dimension",
        "recall_at_k",
        "ndcg_at_k",
    ],
}
CATALOG = [
    {
        "id": "airflow",
        "name": "Airflow 실행 이력",
        "category": "파이프라인",
        "kind": "builder",
        "dialect": "PostgreSQL 집계 · 30초 캐시",
        "description": "로그인 사용자의 DAG 권한으로 전체 기간을 집계합니다. 패널 원본 조회와 짧은 집계 캐시를 공유합니다.",
        "tables": {"dag_runs": SCHEMAS["dag_runs"]},
    },
    {
        "id": "experiments",
        "name": "Model Lab 실험 기록",
        "category": "MLOps",
        "kind": "builder",
        "dialect": "Model Lab 기록 API",
        "description": "Airflow PostgreSQL의 workbench 스키마에 저장된 비교·임베딩 실험 결과를 조회합니다. 학습 실행 상태는 Airflow 실행 이력의 environment_id/workload로 조회합니다.",
        "tables": {k: SCHEMAS[k] for k in ("experiments", "model_results")},
    },
]
PG_METADATA = postgres.Source(
    id="pg_airflow",
    name="Airflow PostgreSQL 직접 조회",
    category="파이프라인",
    connection_id="workbench_ro_airflow",
    tables=[
        "workbench_monitor.dag_runs",
        "workbench_monitor.task_instances",
        "workbench_monitor.dags",
        "workbench_monitor.dag_tags",
        "workbench_monitor.pools",
    ],
)


class Filter(Contract):
    field: str = Field(max_length=100)
    op: Literal["eq", "ne", "contains", "in", "gte", "lte", "not_null"] = "eq"
    value: str = Field(default="", max_length=500)


class Measure(Contract):
    op: Literal["count", "sum", "avg", "min", "max", "p95", "percent"] = "count"
    field: str = Field(default="", max_length=100)
    alias: str = Field(default="value", pattern=r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$")
    match: str = Field(default="success", max_length=100)


class Builder(Contract):
    dataset: Literal["dag_runs", "experiments", "model_results"] = "dag_runs"
    filters: list[Filter] = Field(default_factory=list, max_length=12)
    group_by: list[str] = Field(default_factory=list, max_length=3)
    bucket: str = Field(default="", max_length=100)
    measures: list[Measure] = Field(default_factory=list, max_length=6)
    columns: list[str] = Field(default_factory=list, max_length=30)
    sort: str = Field(default="", max_length=100)
    descending: bool = False
    limit: int = Field(default=2000, ge=1, le=2000)


class Query(Contract):
    ref: str = Field(default="A", pattern=r"^[A-F]$")
    datasource: str = Field(default="airflow", pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    sql: str = Field(default="", max_length=20000)
    builder: Builder = Field(default_factory=Builder)
    enabled: bool = True


class Context(Contract):
    follow_work_scope: bool = True
    run_state: Literal["", "failed", "running", "queued", "success"] = ""
    hours: int = Field(default=24, ge=1, le=2160)
    bucket_seconds: int = Field(default=3600, ge=60, le=86400)
    variables: dict[str, str] = Field(default_factory=dict, max_length=12)
    from_ts: float | None = None
    to_ts: float | None = None

    @model_validator(mode="after")
    def validate_range(self):
        from airflow_workbench.dashboard.boards import QuerySpec

        QuerySpec(sql="SELECT 1", variables=self.variables)
        if (self.from_ts is None) != (self.to_ts is None):
            raise ValueError("시작과 종료 시각을 함께 설정하세요.")
        if self.from_ts is not None and not (
            0 < self.from_ts < self.to_ts and self.to_ts - self.from_ts <= 2160 * 3600
        ):
            raise ValueError("조회 기간은 최대 90일이며 종료는 시작 이후여야 합니다.")
        return self

    def bounds(self):
        end = self.to_ts if self.to_ts is not None else time.time()
        return (
            self.from_ts if self.from_ts is not None else end - self.hours * 3600
        ), end


class QueryRequest(Context):
    query: Query


class Override(Contract):
    name: str = Field(max_length=100)
    color: str = Field(default="#7ba9ff", pattern=r"^#[a-fA-F0-9]{6}$")
    axis: Literal["left", "right"] = "left"


class Visual(Contract):
    x: str = Field(default="state", max_length=100)
    y: str = Field(default="value", max_length=100)
    series: str = Field(default="", max_length=100)
    data_ref: Literal["all", "A", "B", "C", "D", "E", "F"] = "all"
    unit: str = Field(default="", max_length=20)
    decimals: int = Field(default=1, ge=0, le=6)
    color: str = Field(default="#73b4ff", pattern=r"^#[a-fA-F0-9]{6}$")
    legend: bool = True
    stack: bool = False
    smooth: bool = False
    zoom: bool = True
    horizontal: bool = False
    time_axis: bool = False
    reduce: Literal["last", "first", "sum", "avg", "min", "max", "count"] = "last"
    threshold: float | None = None
    threshold_mode: Literal["above", "below"] = "above"
    minimum: float = 0
    maximum: float = 100
    axis_min: float | None = None
    axis_max: float | None = None
    line_width: int = Field(default=2, ge=1, le=6)
    point_size: int = Field(default=5, ge=0, le=20)
    overrides: list[Override] = Field(default_factory=list, max_length=20)
    advanced: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_visual(self):
        if self.maximum <= self.minimum:
            raise ValueError("Gauge 최대값은 최소값보다 커야 합니다.")
        if (
            self.axis_min is not None
            and self.axis_max is not None
            and self.axis_max <= self.axis_min
        ):
            raise ValueError("Y축 최대값은 최소값보다 커야 합니다.")
        # Declarative ECharts presentation options only. No scripts, arbitrary data, or HTML renderers.
        if set(self.advanced) - {
            "grid",
            "xAxis",
            "yAxis",
            "legend",
            "textStyle",
            "backgroundColor",
            "animation",
            "color",
        }:
            raise ValueError(
                "고급 옵션은 grid/xAxis/yAxis/legend/textStyle/backgroundColor/animation/color만 지원합니다."
            )
        if len(json.dumps(self.advanced)) > 20000:
            raise ValueError("고급 옵션은 20KB 이하여야 합니다.")
        return self


class Panel(Contract):
    id: str = Field(
        default_factory=lambda: uuid.uuid4().hex, pattern=r"^[a-zA-Z0-9_-]{1,64}$"
    )
    title: ShortName = "새 패널"
    description: str = Field(default="", max_length=500)
    chart: Literal[
        "stat", "line", "area", "bar", "gauge", "table", "pie", "scatter", "heatmap"
    ] = "bar"
    queries: list[Query] = Field(
        default_factory=lambda: [Query()], min_length=1, max_length=6
    )
    visual: Visual = Field(default_factory=Visual)
    x: int = Field(default=0, ge=0, le=11)
    y: int = Field(default=0, ge=0, le=1000)
    w: int = Field(default=6, ge=2, le=12)
    h: int = Field(default=7, ge=4, le=24)

    @model_validator(mode="after")
    def validate_panel(self):
        if len({q.ref for q in self.queries}) != len(self.queries):
            raise ValueError("쿼리 이름이 중복됩니다.")
        if self.x + self.w > 12:
            raise ValueError("패널 위치가 12열 그리드를 벗어납니다.")
        return self


class Board(Context):
    id: str = Field(
        default_factory=lambda: uuid.uuid4().hex, pattern=r"^[a-zA-Z0-9_-]{1,64}$"
    )
    schema_version: Literal[3] = 3
    title: ShortName = "새 대시보드"
    category: ShortName = "운영"
    description: str = Field(default="", max_length=1000)
    version: int = Field(default=0, ge=0)
    refresh_seconds: Literal[0, 30, 60, 300] = 60
    panels: list[Panel] = Field(default_factory=list, max_length=40)

    @model_validator(mode="after")
    def validate_board(self):
        if len({p.id for p in self.panels}) != len(self.panels):
            raise ValueError("패널 ID가 중복됩니다.")
        return self


def initial_boards():
    def p(title, chart, b, grid_x, grid_y, w, h=7, source="airflow", **v):
        return Panel(
            title=title,
            chart=chart,
            x=grid_x,
            y=grid_y,
            w=w,
            h=h,
            queries=[Query(datasource=source, builder=Builder(**b))],
            visual=Visual(**v),
        )

    count = {"measures": [{"op": "count"}]}
    dag_filter = [{"field": "dag_id", "op": "contains", "value": ":dag"}]
    ops = Board(
        id="operations",
        title="담당 DAG 추세",
        category="파이프라인",
        description="실행 추세, 성공률, 지연 시간을 한곳에서 확인합니다.",
        variables={"dag": ""},
        panels=[
            p(
                "전체 실행",
                "stat",
                {**count, "filters": dag_filter},
                0,
                0,
                4,
                4,
                decimals=0,
            ),
            p(
                "완료 실행 성공률",
                "gauge",
                {
                    "filters": dag_filter
                    + [{"field": "state", "op": "in", "value": "success,failed"}],
                    "measures": [{"op": "percent", "field": "state"}],
                },
                4,
                0,
                4,
                4,
                unit="%",
                threshold=95,
                threshold_mode="below",
                color="#65ddbe",
            ),
            p(
                "실패한 실행",
                "stat",
                {
                    **count,
                    "filters": dag_filter + [{"field": "state", "value": "failed"}],
                },
                8,
                0,
                4,
                4,
                decimals=0,
                color="#ff929a",
            ),
            p(
                "시간별 실행 추세",
                "bar",
                {
                    **count,
                    "filters": dag_filter,
                    "group_by": ["run_ts", "state"],
                    "bucket": "run_ts",
                    "sort": "run_ts",
                },
                0,
                4,
                8,
                8,
                x="run_ts",
                series="state",
                stack=True,
                time_axis=True,
            ),
            p(
                "실행 상태 분포",
                "pie",
                {**count, "filters": dag_filter, "group_by": ["state"]},
                8,
                4,
                4,
                8,
                x="state",
            ),
            p(
                "DAG별 평균 실행 시간",
                "bar",
                {
                    "filters": dag_filter,
                    "group_by": ["dag_id"],
                    "measures": [{"op": "avg", "field": "duration_seconds"}],
                    "sort": "value",
                    "descending": True,
                },
                0,
                12,
                5,
                8,
                x="dag_id",
                horizontal=True,
                unit="s",
            ),
            p(
                "최근 실행",
                "table",
                {
                    "filters": dag_filter,
                    "columns": ["dag_id", "run_id", "state", "duration_seconds"],
                    "sort": "run_ts",
                    "descending": True,
                    "limit": 100,
                },
                5,
                12,
                7,
                8,
            ),
        ],
    )
    lab = Board(
        id="model-lab",
        follow_work_scope=False,
        title="Model Lab 관측",
        category="MLOps",
        description="모델 실험 결과와 외부 워커 DAG 실행을 함께 확인합니다.",
        panels=[
            p(
                "모델 실험 수",
                "stat",
                {**count, "dataset": "experiments"},
                0,
                0,
                4,
                4,
                source="experiments",
                decimals=0,
            ),
            p(
                "평균 응답 지연",
                "stat",
                {
                    "dataset": "model_results",
                    "measures": [{"op": "avg", "field": "latency_seconds"}],
                },
                4,
                0,
                4,
                4,
                source="experiments",
                unit="s",
            ),
            p(
                "Model Lab DAG 실행",
                "stat",
                {
                    **count,
                    "filters": [{"field": "environment_id", "op": "not_null"}],
                },
                8,
                0,
                4,
                4,
                decimals=0,
            ),
            p(
                "모델별 응답 지연",
                "line",
                {"dataset": "model_results", "sort": "created"},
                0,
                4,
                7,
                8,
                source="experiments",
                x="created",
                y="latency_seconds",
                series="model",
                time_axis=True,
                unit="s",
            ),
            p(
                "임베딩 검색 품질",
                "table",
                {
                    "dataset": "model_results",
                    "filters": [{"field": "dimension", "op": "not_null"}],
                    "columns": [
                        "model",
                        "dimension",
                        "recall_at_k",
                        "ndcg_at_k",
                        "latency_seconds",
                    ],
                },
                7,
                4,
                5,
                8,
                source="experiments",
            ),
        ],
    )
    return [ops, lab]


class Preferences(Contract):
    last_board_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")


def list_boards(owner):
    with Store().connection() as db:
        db.lock("studio:" + owner)
        if not db.execute(
            "SELECT 1 FROM studio_preferences WHERE owner=?", (owner,)
        ).fetchone():
            for board in initial_boards():
                board.version = 1
                db.execute(
                    "INSERT INTO studio_boards VALUES (?,?,?,1) ON CONFLICT(owner,id) DO NOTHING",
                    (owner, board.id, board.model_dump_json()),
                )
            db.execute(
                "INSERT INTO studio_preferences(owner,last_board_id) VALUES (?,?)",
                (owner, "operations"),
            )
        return [
            json.loads(r[0])
            for r in db.execute(
                "SELECT value FROM studio_boards WHERE owner=? ORDER BY id", (owner,)
            )
        ]


def save_board(owner, board):
    with Store().connection() as db:
        db.lock("studio:" + owner)
        row = db.execute(
            "SELECT value,version FROM studio_boards WHERE owner=? AND id=?",
            (owner, board.id),
        ).fetchone()
        if row is None and board.version:
            raise NotFound("내 대시보드를 찾을 수 없습니다.")
        version = row[1] if row else 0
        if version != board.version:
            raise Conflict(
                "다른 창에서 저장했습니다. 변경 내용을 내보낸 뒤 다시 불러오세요."
            )
        if row:
            db.execute(
                "INSERT INTO studio_revisions VALUES (?,?,?,?)",
                (owner, board.id, version, row[0]),
            )
        value = board.model_dump()
        value["version"] = version + 1
        db.execute(
            "INSERT INTO studio_boards VALUES (?,?,?,?) ON CONFLICT(owner,id) DO UPDATE SET value=excluded.value,version=excluded.version",
            (owner, board.id, json.dumps(value), version + 1),
        )
        # Selection and board save commit together, including newly created boards.
        db.execute(
            "INSERT INTO studio_preferences(owner,last_board_id) VALUES (?,?) "
            "ON CONFLICT(owner) DO UPDATE SET last_board_id=excluded.last_board_id",
            (owner, board.id),
        )
        # Keep the most recent 20 revisions per board.
        db.execute(
            "DELETE FROM studio_revisions WHERE owner=? AND board_id=? AND version<?",
            (owner, board.id, max(0, version - 19)),
        )
    return value


def revisions(owner, board_id):
    with Store().connection() as db:
        if not db.execute(
            "SELECT 1 FROM studio_boards WHERE owner=? AND id=?", (owner, board_id)
        ).fetchone():
            raise NotFound("내 대시보드를 찾을 수 없습니다.")
        return [
            json.loads(r[0])
            for r in db.execute(
                "SELECT value FROM studio_revisions WHERE owner=? AND board_id=? ORDER BY version DESC",
                (owner, board_id),
            )
        ]


def templates(owner):
    with Store().connection() as db:
        return [
            json.loads(r[0])
            for r in db.execute(
                "SELECT value FROM studio_templates WHERE owner=? ORDER BY id", (owner,)
            )
        ]


def save_template(owner, panel):
    with Store().connection() as db:
        db.execute(
            "INSERT INTO studio_templates VALUES (?,?,?) ON CONFLICT(owner,id) DO UPDATE SET value=excluded.value",
            (owner, panel.id, panel.model_dump_json()),
        )
    return panel


def preferences(owner):
    with Store().connection() as db:
        row = db.execute(
            "SELECT last_board_id,legacy_imported FROM studio_preferences WHERE owner=?",
            (owner,),
        ).fetchone()
    return dict(row) if row else {"last_board_id": None, "legacy_imported": 0}


def save_preferences(owner, spec):
    with Store().connection() as db:
        db.lock("studio:" + owner)
        if not db.execute(
            "SELECT 1 FROM studio_boards WHERE owner=? AND id=?",
            (owner, spec.last_board_id),
        ).fetchone():
            raise NotFound("내 대시보드를 찾을 수 없습니다.")
        db.execute(
            "INSERT INTO studio_preferences(owner,last_board_id) VALUES (?,?) "
            "ON CONFLICT(owner) DO UPDATE SET last_board_id=excluded.last_board_id",
            (owner, spec.last_board_id),
        )
    return spec


def legacy_archive(db=None):
    """Pre-account data has no verifiable owner. Only the admin API exposes it."""
    if db is None:
        with Store().connection() as connection:
            return legacy_archive(connection)
    return {
        kind: [
            json.loads(r[0])
            for r in db.execute(
                "SELECT value FROM documents WHERE key LIKE ? ORDER BY key",
                (f"studio:{prefix}:%",),
            )
        ]
        for kind, prefix in (
            ("boards", "board"),
            ("revisions", "revision"),
            ("templates", "template"),
        )
    }


def import_legacy(owner):
    """Atomically copy the original archive once per account; leave originals intact."""
    list_boards(owner)
    with Store().connection() as db:
        db.lock("studio:" + owner)
        if db.execute(
            "SELECT legacy_imported FROM studio_preferences WHERE owner=?", (owner,)
        ).fetchone()[0]:
            raise Conflict("기존 공용 구성은 이미 내 대시보드로 가져왔습니다.")
        archive = legacy_archive(db)
        imported = []
        for source in archive["boards"]:
            candidate = Board.model_validate(source)
            old_id, candidate.id = candidate.id, uuid.uuid4().hex
            candidate.version = max(1, candidate.version)
            db.execute(
                "INSERT INTO studio_boards VALUES (?,?,?,?)",
                (owner, candidate.id, candidate.model_dump_json(), candidate.version),
            )
            history = sorted(
                (
                    r
                    for r in archive["revisions"]
                    if r["id"] == old_id and r["version"] < candidate.version
                ),
                key=lambda r: r["version"],
            )[-20:]
            for revision in history:
                saved = Board.model_validate({**revision, "id": candidate.id})
                db.execute(
                    "INSERT INTO studio_revisions VALUES (?,?,?,?)",
                    (owner, candidate.id, saved.version, saved.model_dump_json()),
                )
            imported.append(candidate.model_dump())
        for source in archive["templates"]:
            panel = Panel.model_validate({**source, "id": uuid.uuid4().hex})
            db.execute(
                "INSERT INTO studio_templates VALUES (?,?,?)",
                (owner, panel.id, panel.model_dump_json()),
            )
        if imported or archive["templates"]:
            db.execute(
                "UPDATE studio_preferences SET legacy_imported=1 WHERE owner=?",
                (owner,),
            )
    return {"boards": imported, "templates": len(archive["templates"])}


def stamp(value):
    return (
        datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        if value
        else None
    )


def experiment_records(since, until):
    with Store().connection() as db:
        total = db.execute(
            "SELECT count(*) FROM experiments WHERE created BETWEEN ? AND ?",
            (since, until),
        ).fetchone()[0]
        records = db.execute(
            "SELECT id,name,kind,status,created,finished,result FROM experiments WHERE created BETWEEN ? AND ? ORDER BY created DESC LIMIT 5000",
            (since, until),
        ).fetchall()
    rows, results = [], []
    for r in records:
        rows.append({k: r[k] for k in SCHEMAS["experiments"]})
        for model in json.loads(r["result"] or "{}").get("models", []):
            item = {k: model.get(k) for k in SCHEMAS["model_results"]}
            item.update(
                experiment_id=r["id"],
                created=r["created"],
                **{
                    k: (model.get("metrics") or {}).get(k)
                    for k in ("recall_at_k", "ndcg_at_k")
                },
            )
            results.append(item)
    tables = {"experiments": rows, "model_results": results}
    return tables, len(rows), total


async def records(source, context, fetch):
    since, until = context.bounds()
    if source == "airflow":
        from airflow_workbench.dashboard.mlops_source import environment_index

        targets = environment_index()
        rows, total = [], 0
        for offset in range(0, 5000, 100):
            batch = await fetch(
                "/dags/~/dagRuns",
                {
                    "limit": 100,
                    "offset": offset,
                    "order_by": "-run_after",
                    "run_after_gte": datetime.fromtimestamp(
                        since, timezone.utc
                    ).isoformat(),
                    "run_after_lte": datetime.fromtimestamp(
                        until, timezone.utc
                    ).isoformat(),
                },
            )
            items = batch.get("dag_runs", [])
            total = batch.get("total_entries", len(items))
            for r in items:
                start, end = stamp(r.get("start_date")), stamp(r.get("end_date"))
                rows.append(
                    dict(
                        dag_id=r["dag_id"],
                        run_id=r["dag_run_id"],
                        state=r.get("state"),
                        run_ts=stamp(r.get("run_after")),
                        start_ts=start,
                        end_ts=end,
                        duration_seconds=(
                            end - start
                            if start is not None and end is not None and end >= start
                            else None
                        ),
                        environment_id=targets.get(r["dag_id"], (None, None))[0],
                        workload=targets.get(r["dag_id"], (None, None))[1],
                    )
                )
            if len(items) < 100 or len(rows) >= total:
                break
        tables = {"dag_runs": rows}
    elif source == "experiments":
        tables, row_count, total = await protection.run(
            experiment_records, since, until
        )
        rows = tables["experiments"]
    else:
        raise ValueError("등록되지 않은 데이터소스입니다.")
    return tables, {
        "source_rows": len(rows),
        "total_rows": total,
        "truncated": total > len(rows),
        "from_ts": since,
        "to_ts": until,
    }


def build_result(builder, tables, context):
    if builder.dataset not in tables:
        raise ValueError("이 데이터소스에 해당 데이터셋이 없습니다.")
    fields = SCHEMAS[builder.dataset]
    required = (
        builder.group_by
        + [f.field for f in builder.filters]
        + [m.field for m in builder.measures if m.field]
    )
    if builder.bucket:
        required.append(builder.bucket)
        if builder.bucket not in builder.group_by:
            raise ValueError("시간 버킷 필드를 그룹에도 추가하세요.")
    if set(required) - set(fields):
        raise ValueError("데이터셋에 없는 필드입니다.")
    aliases = [m.alias for m in builder.measures]
    if len(set(aliases + builder.group_by)) != len(aliases + builder.group_by):
        raise ValueError("집계 결과 이름과 그룹 필드는 중복될 수 없습니다.")
    for m in builder.measures:
        if m.op != "count" and not m.field:
            raise ValueError("집계할 필드를 선택하세요.")

    def matches(row, f):
        value = f.value
        if value.startswith(":"):
            if value[1:] not in context.variables:
                raise ValueError(f"보드 변수 {value}를 등록하세요.")
            value = context.variables[value[1:]]
        actual = row.get(f.field)
        if f.op == "not_null":
            return actual is not None
        if f.op == "contains":
            return value.lower() in str(actual or "").lower()
        if f.op == "in":
            return str(actual) in [x.strip() for x in value.split(",")]
        if f.op == "eq":
            return str(actual) == value
        if f.op == "ne":
            return str(actual) != value
        try:
            return (
                float(actual) >= float(value)
                if f.op == "gte"
                else float(actual) <= float(value)
            )
        except (ValueError, TypeError):
            return False

    rows = [
        dict(r)
        for r in tables[builder.dataset]
        if all(matches(r, f) for f in builder.filters)
    ]
    if builder.bucket:
        for r in rows:
            val = r[builder.bucket]
            r[builder.bucket] = (
                datetime.fromtimestamp(
                    math.floor(float(val) / context.bucket_seconds)
                    * context.bucket_seconds,
                    timezone.utc,
                ).isoformat()
                if val is not None
                else None
            )
    if builder.group_by or builder.measures:
        groups = {}
        for r in rows:
            key = tuple(r[k] for k in builder.group_by)
            groups.setdefault(key, []).append(r)
        if not rows and not builder.group_by:
            groups[()] = []
        output = []
        for key, items in groups.items():
            row = dict(zip(builder.group_by, key))
            for m in builder.measures:
                vals = [r[m.field] for r in items if r.get(m.field) is not None]
                nums = [
                    float(v)
                    for v in vals
                    if isinstance(v, (int, float)) and math.isfinite(v)
                ]
                if m.op == "count":
                    value = len(items)
                elif m.op == "percent":
                    value = (
                        100 * sum(str(v) == m.match for v in vals) / len(vals)
                        if vals
                        else None
                    )
                elif not nums:
                    value = None
                elif m.op == "sum":
                    value = sum(nums)
                elif m.op == "avg":
                    value = statistics.mean(nums)
                elif m.op == "min":
                    value = min(nums)
                elif m.op == "max":
                    value = max(nums)
                else:
                    value = sorted(nums)[max(0, math.ceil(len(nums) * 0.95) - 1)]
                row[m.alias] = value
            output.append(row)
        rows = output
        available = builder.group_by + aliases
    else:
        available = fields
    if builder.sort:
        if builder.sort not in available:
            raise ValueError("정렬 필드를 결과에서 찾을 수 없습니다.")
        nonnull = [r for r in rows if r.get(builder.sort) is not None]
        nulls = [r for r in rows if r.get(builder.sort) is None]
        rows = (
            sorted(nonnull, key=lambda r: r[builder.sort], reverse=builder.descending)
            + nulls
        )
    columns = builder.columns or available
    if set(columns) - set(available):
        raise ValueError("집계 결과에 없는 표시 필드입니다.")
    return {
        "columns": columns,
        "rows": [{c: r.get(c) for c in columns} for r in rows[: builder.limit]],
        "limited": len(rows) > builder.limit,
    }


async def run_query(q, context, fetch, cache=None, allow_postgres=False, resolved=None):
    if resolved is not None and q.datasource == "airflow":
        from airflow_workbench.dashboard.aggregate import identity

        result = resolved.get(identity(q.builder))
        if result is not None:
            return {**result, "ref": q.ref}
    started = time.monotonic()
    since, until = context.bounds()
    if q.datasource.startswith("pg_"):
        if not allow_postgres:
            raise ValueError(
                "직접 SQL은 Workbench 편집 권한이 필요합니다. API 빌더는 DAG별 조회 권한을 유지합니다."
            )
        result = await protection.run(
            postgres.execute,
            q.datasource,
            q.sql,
            {
                **context.variables,
                "from_ts": since,
                "to_ts": until,
                "bucket_seconds": context.bucket_seconds,
            },
        )
    else:
        cache = cache if cache is not None else {}
        if q.datasource not in cache:
            try:
                cache[q.datasource] = await asyncio.wait_for(
                    records(q.datasource, context, fetch), timeout=15
                )
            except TimeoutError:
                cache[q.datasource] = ValueError(
                    "원본 조회가 15초를 초과했습니다. 기간을 줄여 다시 실행하세요."
                )
            except Exception as exc:
                cache[q.datasource] = exc
        if isinstance(cache[q.datasource], Exception):
            raise cache[q.datasource]
        tables, meta = cache[q.datasource]
        if meta.get("truncated") and (q.builder.group_by or q.builder.measures):
            raise ValueError(
                "전체 기록을 읽지 못해 정확한 집계를 표시할 수 없습니다. 조회 기간을 줄여 주세요."
            )
        result = {
            **await protection.run(build_result, q.builder, tables, context),
            **meta,
        }
    if len(json.dumps(result, default=str).encode()) > 1024 * 1024:
        raise ValueError("결과가 1MB를 초과합니다. 필터와 집계를 추가하세요.")
    return {
        **result,
        "ref": q.ref,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
    }


async def run_board(
    board, fetch, allow_postgres=False, resolved=None, assigned_scope=False
):
    # Freeze the time window so panels and SQL queries observe identical boundaries.
    since, until = board.bounds()
    context = Context(
        hours=board.hours,
        bucket_seconds=board.bucket_seconds,
        variables=board.variables,
        from_ts=since,
        to_ts=until,
    )
    cache, result = {}, {}
    started = time.monotonic()
    for panel in board.panels:
        frames = []
        for q in panel.queries:
            if not q.enabled:
                continue
            try:
                if assigned_scope and q.datasource != "airflow":
                    raise ValueError(
                        "이 데이터소스에는 담당 DAG 필터를 적용할 수 없습니다. 별도 분석 범위에서 조회하세요."
                    )
                if time.monotonic() - started > 25:
                    raise ValueError(
                        "대시보드 조회 시간이 초과되었습니다. 패널 또는 조회 기간을 줄이세요."
                    )
                frames.append(
                    await run_query(q, context, fetch, cache, allow_postgres, resolved)
                )
            except protection.Deferred:
                raise
            except Exception as exc:
                from fastapi import HTTPException

                message = (
                    exc.detail
                    if isinstance(exc, HTTPException)
                    else (
                        str(exc)
                        if isinstance(exc, ValueError)
                        else "데이터 조회 중 오류가 발생했습니다. 서버 로그를 확인하세요."
                    )
                )
                frames.append(
                    {"ref": q.ref, "error": message, "columns": [], "rows": []}
                )
        result[panel.id] = frames
    return result
