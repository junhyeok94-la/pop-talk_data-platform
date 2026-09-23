"""프로젝트별 PostgreSQL 모니터링 데이터소스. 전용 읽기 계정만 허용한다."""

import json
import re
import time
from contextlib import contextmanager
from decimal import Decimal

import psycopg
import sqlparse
from sqlparse import tokens as T
from sqlparse.sql import Function, Identifier
from pydantic import Field

from airflow_workbench.dashboard import protection
from airflow_workbench.shared.contracts import Contract, ShortName
from airflow_workbench.dashboard.database import Store

FUNCTIONS = {
    "abs",
    "avg",
    "count",
    "sum",
    "min",
    "max",
    "round",
    "coalesce",
    "nullif",
    "lower",
    "upper",
    "length",
    "substring",
    "substr",
    "replace",
    "trim",
    "ltrim",
    "rtrim",
    "date_trunc",
    "date_part",
    "extract",
    "to_timestamp",
    "to_char",
    "cast",
    "greatest",
    "least",
    "floor",
    "ceil",
    "ceiling",
    "power",
    "sqrt",
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "ntile",
    "percentile_cont",
    "percentile_disc",
    "stddev",
    "variance",
    "string_agg",
    "array_agg",
    "jsonb_extract_path_text",
}


class Source(Contract):
    id: str = Field(pattern=r"^pg_[a-z][a-z0-9_]{0,50}$")
    name: ShortName
    category: ShortName = "데이터웨어하우스"
    connection_id: str = Field(pattern=r"^workbench_ro_[a-z][a-z0-9_]{0,50}$")
    tables: list[str] = Field(min_length=1, max_length=32)


def configured():
    with Store().connection() as db:
        rows = db.execute(
            "SELECT value FROM documents WHERE key LIKE 'datasource:%' ORDER BY key"
        ).fetchall()
    return [Source.model_validate_json(row[0]) for row in rows]


def get_source(source_id):
    for source in configured():
        if source.id == source_id:
            return source
    if source_id == "pg_airflow":
        from airflow_workbench.dashboard.studio import PG_METADATA

        return PG_METADATA
    raise ValueError("등록되지 않은 PostgreSQL 데이터소스입니다.")


@contextmanager
def connect(source):
    from airflow.models.connection import Connection

    try:
        try:
            conf = Connection.get_connection_from_secrets(source.connection_id)
        except Exception as exc:
            raise ValueError(
                "Airflow에 해당 전용 읽기 Connection을 먼저 등록하세요."
            ) from exc
        if conf.conn_type not in {"postgres", "postgresql"}:
            raise ValueError("PostgreSQL Connection을 사용하세요.")
        with psycopg.connect(
            host=conf.host,
            port=conf.port or 5432,
            dbname=conf.schema,
            user=conf.login,
            password=conf.password,
            connect_timeout=2,
            application_name="workbench_dashboard_sql",
            options=f"-c statement_timeout={protection.policy.query_ms} -c lock_timeout=100 -c idle_in_transaction_session_timeout=2500 -c max_parallel_workers_per_gather=0 -c work_mem=4MB -c search_path=pg_catalog",
        ) as db:
            db.read_only = True
            with db.cursor() as cur:
                cur.execute(
                    "SELECT rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=current_user"
                )
                if any(cur.fetchone()):
                    raise ValueError(
                        "DB 관리자 계정은 사용할 수 없습니다. 전용 SELECT 계정을 연결하세요."
                    )
                cur.execute("SHOW default_transaction_read_only")
                if cur.fetchone()[0] != "on":
                    raise ValueError(
                        "전용 계정에 default_transaction_read_only=on 설정이 필요합니다."
                    )
            yield db
    except psycopg.Error as exc:
        # 연결 문자열/비밀번호와 DB 내부 에러 context를 응답에 노출하지 않는다.
        raise ValueError(
            f"PostgreSQL 조회 실패 ({exc.sqlstate or 'connection'}). 연결·SELECT 권한·쿼리와 {protection.policy.query_ms}ms 제한을 확인하세요."
        ) from exc


def inspect(source):
    schema = {}
    with connect(source) as db, db.cursor() as cur:
        for table in source.tables:
            if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", table):
                raise ValueError("테이블은 schema.table 형식으로 입력하세요.")
            owner, name = table.split(".")
            if owner in {"pg_catalog", "information_schema"}:
                raise ValueError("프로젝트 모니터링 테이블/뷰를 등록하세요.")
            cur.execute(
                "SELECT column_name,data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                (owner, name),
            )
            cols = cur.fetchall()
            if not cols:
                raise ValueError(f"{table}: 조회 권한 또는 테이블을 확인하세요.")
            cur.execute(
                "SELECT has_table_privilege(current_user,%s,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')",
                (table,),
            )
            if cur.fetchone()[0]:
                raise ValueError(
                    "모니터링 테이블에 쓰기 권한이 있는 계정은 사용할 수 없습니다."
                )
            schema[table] = [f"{name} {kind}" for name, kind in cols]
    return {
        "id": source.id,
        "name": source.name,
        "category": source.category,
        "dialect": "PostgreSQL · read only · 2s",
        "kind": "sql",
        "tables": schema,
    }


def save(source):
    catalog = inspect(source)
    with Store().connection() as db:
        db.execute(
            "INSERT INTO documents(key,value,version) VALUES (?,?,1) ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=documents.version+1",
            ("datasource:" + source.id, source.model_dump_json()),
        )
    return catalog


def prepare_sql(sql, params):
    statements = sqlparse.parse(sql)
    if len(statements) != 1 or statements[0].get_type() != "SELECT":
        raise ValueError("SELECT 또는 SELECT로 끝나는 CTE 하나만 실행할 수 있습니다.")
    statement = statements[0]
    for token in statement.flatten():
        if token.ttype in T.Keyword and token.normalized in {
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
            "CREATE",
            "ALTER",
            "DROP",
            "TRUNCATE",
            "COPY",
            "CALL",
            "DO",
            "SET",
            "RESET",
            "GRANT",
            "REVOKE",
            "INTO",
            "LOCK",
            "RECURSIVE",
        }:
            raise ValueError("읽기 전용 SELECT만 허용합니다.")

    def functions(group):
        for token in group.tokens:
            if isinstance(token, Function):
                if (token.get_name() or "").lower() not in FUNCTIONS:
                    raise ValueError(
                        "허용되지 않은 SQL 함수입니다. 집계/시간/문자열/윈도 함수를 사용하세요."
                    )
                if token.get_parent_name():
                    raise ValueError(
                        "스키마가 붙은 사용자 정의 함수는 지원하지 않습니다."
                    )
            if (
                isinstance(token, Identifier)
                and any(isinstance(t, Function) for t in token.tokens)
                and token.get_parent_name()
            ):
                raise ValueError("스키마가 붙은 함수는 지원하지 않습니다.")
            if token.is_group:
                functions(token)

    functions(statement)
    pieces = []
    for token in statement.flatten():
        if token.ttype in T.Name.Placeholder:
            key = token.value.removeprefix(":")
            if key not in params:
                raise ValueError(f"쿼리 변수 {key}를 보드에 등록하세요.")
            pieces.append("%(" + key + ")s")
        else:
            pieces.append(token.value.replace("%", "%%"))
    return "".join(pieces).rstrip().removesuffix(";")


def execute(source_id, sql, params):
    started = time.monotonic()
    source = get_source(source_id)
    # 연결 계정과 등록된 테이블의 권한을 매 실행 시 다시 확인한다.
    inspect(source)
    prepared = prepare_sql(sql, params)
    with connect(source) as db, db.cursor(name="workbench_query") as cur:
        cur.itersize = 100
        cur.execute(prepared, params)
        columns = [c.name for c in cur.description]
        if len(columns) > 50 or len(set(columns)) != len(columns):
            raise ValueError("컬럼은 중복 없이 최대 50개입니다.")
        rows = []
        size = 0
        for row in cur:
            if len(rows) == 2000:
                break
            item = {
                k: (
                    float(v)
                    if isinstance(v, Decimal)
                    else v.isoformat() if hasattr(v, "isoformat") else v
                )
                for k, v in zip(columns, row)
            }
            size += len(json.dumps(item, ensure_ascii=False, default=str).encode())
            if size > 1048576:
                raise ValueError("쿼리 결과가 1MB를 초과합니다. 집계해 주세요.")
            rows.append(item)
            if time.monotonic() - started > 5:
                raise ValueError("전체 조회 시간이 5초를 초과했습니다.")
    return {
        "columns": columns,
        "rows": rows,
        "limited": len(rows) == 2000,
        "truncated": False,
        "elapsed_ms": round((time.monotonic() - started) * 1000),
        "datasource": source_id,
        "sampled_at": time.time(),
    }
