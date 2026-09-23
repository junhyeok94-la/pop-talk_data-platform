"""카테고리별 보드와 사용자가 편집하는 SQL/시각화 계약."""

import json
import re
import uuid
from typing import Literal

from pydantic import Field, model_validator

from airflow_workbench.shared.contracts import Contract, ShortName
from airflow_workbench.dashboard.database import Conflict
from airflow_workbench.dashboard.legacy_store import Store


class QuerySpec(Contract):
    datasource: str = Field(default="airflow", pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    sql: str = Field(min_length=1, max_length=20000)
    hours: int = Field(default=24, ge=1, le=2160)
    bucket_seconds: int = Field(default=3600, ge=60, le=86400)
    variables: dict[str, str] = Field(default_factory=dict, max_length=12)

    @model_validator(mode="after")
    def variable_names(self):
        for key, value in self.variables.items():
            if (
                not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", key)
                or key in {"from_ts", "to_ts", "bucket_seconds"}
                or len(value) > 500
            ):
                raise ValueError(
                    "변수 이름/값이 올바르지 않습니다. from_ts/to_ts/bucket_seconds는 예약 변수입니다."
                )
        return self


class QueryPanel(Contract):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    title: ShortName
    description: str = Field(default="", max_length=500)
    datasource: str = Field(default="airflow", pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    sql: str = Field(min_length=1, max_length=20000)
    chart: Literal["stat", "line", "bar", "area", "table", "gauge"] = "table"
    x: str = Field(default="time", max_length=100)
    y: str = Field(default="value", max_length=100)
    series: str = Field(default="", max_length=100)
    unit: str = Field(default="", max_length=20)
    decimals: int = Field(default=1, ge=0, le=6)
    color: str = Field(default="#65ddbe", pattern=r"^#[a-fA-F0-9]{6}$")
    threshold: float | None = None
    threshold_mode: Literal["above", "below"] = "above"
    minimum: float = 0
    maximum: float = 100
    width: Literal[3, 4, 6, 8, 9, 12] = 6
    height: int = Field(default=280, ge=160, le=800)

    @model_validator(mode="after")
    def gauge_range(self):
        if self.maximum <= self.minimum:
            raise ValueError("최댓값은 최솟값보다 커야 합니다.")
        return self


class Board(Contract):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    title: ShortName
    category: ShortName = "운영"
    description: str = Field(default="", max_length=1000)
    version: int = Field(default=0, ge=0)
    hours: int = Field(default=24, ge=1, le=2160)
    refresh_seconds: Literal[0, 30, 60, 300] = 60
    bucket_seconds: int = Field(default=3600, ge=60, le=86400)
    variables: dict[str, str] = Field(default_factory=dict, max_length=12)
    panels: list[QueryPanel] = Field(default_factory=list, max_length=40)

    @model_validator(mode="after")
    def valid_board(self):
        QuerySpec(sql="SELECT 1", variables=self.variables)
        if len({p.id for p in self.panels}) != len(self.panels):
            raise ValueError("패널 ID가 중복됩니다.")
        return self


def panel(title, sql, chart="table", **kwargs):
    return QueryPanel(id=uuid.uuid4().hex, title=title, sql=sql, chart=chart, **kwargs)


def initial_boards():
    old = Store().dashboard()
    return [
        Board(
            id="operations",
            title=old["title"],
            category="파이프라인",
            variables={"dag": ""},
            panels=[
                panel(
                    "DAG 실행",
                    "SELECT count(*) AS value FROM dag_runs WHERE dag_id LIKE '%' || :dag || '%'",
                    "stat",
                    width=4,
                    height=180,
                    decimals=0,
                ),
                panel(
                    "완료 실행 성공률",
                    "SELECT 100.0 * sum(state = 'success') / nullif(sum(state IN ('success','failed')),0) AS value FROM dag_runs WHERE dag_id LIKE '%' || :dag || '%'",
                    "gauge",
                    width=4,
                    height=180,
                    unit="%",
                    threshold=95,
                    threshold_mode="below",
                ),
                panel(
                    "실패한 실행",
                    "SELECT count(*) AS value FROM dag_runs WHERE state = 'failed' AND dag_id LIKE '%' || :dag || '%'",
                    "stat",
                    width=4,
                    height=180,
                    color="#ff929a",
                    decimals=0,
                    threshold=1,
                ),
                panel(
                    "시간별 실행 상태",
                    "SELECT datetime(cast(run_ts / :bucket_seconds AS integer) * :bucket_seconds, 'unixepoch') AS time, state AS series, count(*) AS value FROM dag_runs WHERE dag_id LIKE '%' || :dag || '%' GROUP BY 1,2 ORDER BY 1",
                    "bar",
                    series="series",
                    width=8,
                ),
                panel(
                    "DAG별 평균 소요시간",
                    "SELECT dag_id AS time, round(avg(duration_seconds),1) AS value FROM dag_runs WHERE duration_seconds >= 0 AND dag_id LIKE '%' || :dag || '%' GROUP BY dag_id ORDER BY value DESC",
                    "bar",
                    width=4,
                    unit="s",
                ),
                panel(
                    "실행 상세",
                    "SELECT dag_id, run_id, state, datetime(start_ts,'unixepoch') AS started_utc, duration_seconds FROM dag_runs WHERE dag_id LIKE '%' || :dag || '%' ORDER BY run_ts DESC",
                    width=12,
                    height=340,
                ),
            ],
        ),
        Board(
            id="model-operations",
            title="모델 실험 모니터링",
            category="MLOps",
            panels=[
                panel(
                    "실험 수",
                    "SELECT count(*) AS value FROM experiments",
                    "stat",
                    datasource="experiments",
                    width=4,
                    height=180,
                    decimals=0,
                ),
                panel(
                    "실험 성공률",
                    "SELECT 100.0 * sum(status='success')/nullif(count(*),0) AS value FROM experiments",
                    "gauge",
                    datasource="experiments",
                    width=4,
                    height=180,
                    unit="%",
                ),
                panel(
                    "평균 지연시간",
                    "SELECT avg(latency_seconds) AS value FROM model_results",
                    "stat",
                    datasource="experiments",
                    width=4,
                    height=180,
                    unit="s",
                ),
                panel(
                    "모델별 실험 지연시간",
                    "SELECT datetime(created,'unixepoch') AS time, model AS series, latency_seconds AS value FROM model_results ORDER BY created",
                    "line",
                    datasource="experiments",
                    series="series",
                    width=12,
                    unit="s",
                ),
                panel(
                    "검색 평가 결과",
                    "SELECT model, dimension, recall_at_k, ndcg_at_k, latency_seconds FROM model_results WHERE dimension IS NOT NULL",
                    datasource="experiments",
                    width=12,
                ),
            ],
        ),
    ]


def list_boards():
    store = Store()
    defaults = initial_boards()
    with store.connection() as db:
        db.lock("legacy-boards")
        if not db.execute(
            "SELECT 1 FROM documents WHERE key LIKE 'board:%' LIMIT 1"
        ).fetchone():
            for board in defaults:
                value = board.model_dump()
                value["version"] = 1
                db.execute(
                    "INSERT INTO documents VALUES (?, ?, 1)",
                    ("board:" + board.id, json.dumps(value)),
                )
        return [
            json.loads(row[0])
            for row in db.execute(
                "SELECT value FROM documents WHERE key LIKE 'board:%' ORDER BY key"
            )
        ]


def save_board(board):
    with Store().connection() as db:
        db.lock("legacy-boards")
        row = db.execute(
            "SELECT version FROM documents WHERE key=?", ("board:" + board.id,)
        ).fetchone()
        version = row[0] if row else 0
        if version != board.version:
            raise Conflict(
                "다른 사용자가 보드를 변경했습니다. 다시 불러온 뒤 저장하세요."
            )
        value = board.model_dump()
        value["version"] = version + 1
        db.execute(
            "INSERT INTO documents VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=excluded.version",
            ("board:" + board.id, json.dumps(value), version + 1),
        )
    return value
