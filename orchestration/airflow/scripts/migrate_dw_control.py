#!/usr/bin/env python3
"""플랫폼 PostgreSQL에 ``dw_control`` 전용 role과 versioned schema를 만든다.

마이그레이션 접속자는 로컬 DB owner 권한을 사용하지만, DAG가 사용하는 runtime role에는
DDL·DELETE·TRUNCATE 권한이나 다른 schema role membership을 주지 않는다.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg
from psycopg import sql

OWNER_ROLE = "dw_control_owner"
RUNTIME_ROLE = "dw_control_runtime"


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"필수 환경변수가 없습니다: {name}")
    return value


def _migration_paths() -> tuple[Path, ...]:
    root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", Path(__file__).resolve().parents[1] / "modules"))
    directory = root / "pipelines" / "orchestration" / "sql"
    paths = tuple(sorted(directory.glob("[0-9][0-9][0-9]_*.sql")))
    if not paths:
        raise RuntimeError("dw_control migration 파일이 없습니다")
    versions = [int(path.name.split("_", 1)[0]) for path in paths]
    if versions != list(range(1, len(paths) + 1)):
        raise RuntimeError("dw_control migration version이 1부터 연속적이지 않습니다")
    return paths


def _ensure_roles(connection: psycopg.Connection, runtime_password: str) -> None:
    """고정 role만 생성한다. 비밀번호는 SQL 문자열 조합이나 로그에 노출하지 않는다."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (OWNER_ROLE,))
        if cursor.fetchone() is None:
            cursor.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(OWNER_ROLE)))
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (RUNTIME_ROLE,))
        if cursor.fetchone() is None:
            cursor.execute(
                sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
                    sql.Identifier(RUNTIME_ROLE), sql.Literal(runtime_password)
                )
            )
        else:
            cursor.execute(
                sql.SQL("ALTER ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
                    sql.Identifier(RUNTIME_ROLE), sql.Literal(runtime_password)
                )
            )
        cursor.execute(
            sql.SQL("REVOKE {} FROM {}").format(
                sql.Identifier(OWNER_ROLE), sql.Identifier(RUNTIME_ROLE)
            )
        )
        cursor.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(connection.info.dbname), sql.Identifier(RUNTIME_ROLE)
            )
        )
        cursor.execute(
            sql.SQL("GRANT CONNECT, CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(connection.info.dbname), sql.Identifier(OWNER_ROLE)
            )
        )
        cursor.execute(
            """SELECT rolsuper,rolcreatedb,rolcreaterole,rolinherit
               FROM pg_roles WHERE rolname=%s""",
            (RUNTIME_ROLE,),
        )
        if cursor.fetchone() != (False, False, False, False):
            raise RuntimeError("dw_control runtime role에 금지된 cluster 권한이 있습니다")
        cursor.execute(
            """SELECT 1 FROM pg_auth_members m
               JOIN pg_roles member_role ON member_role.oid=m.member
               WHERE member_role.rolname=%s LIMIT 1""",
            (RUNTIME_ROLE,),
        )
        if cursor.fetchone() is not None:
            raise RuntimeError("dw_control runtime role은 다른 role membership을 가질 수 없습니다")


def migrate(
    connection: psycopg.Connection,
    *,
    runtime_password: str,
    migration_paths: tuple[Path, ...],
) -> tuple[tuple[int, str], ...]:
    applied: list[tuple[int, str]] = []
    with connection.transaction():
        _ensure_roles(connection, runtime_password)
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('dw_control.schema_migrations')")
            table_exists = cursor.fetchone()[0] is not None
            for migration_path in migration_paths:
                version = int(migration_path.name.split("_", 1)[0])
                name = migration_path.stem.split("_", 1)[1].replace("_", " ")
                source = migration_path.read_bytes()
                digest = hashlib.sha256(source).hexdigest()
                existing = None
                if table_exists:
                    cursor.execute(
                        "SELECT sha256 FROM dw_control.schema_migrations WHERE version = %s",
                        (version,),
                    )
                    existing = cursor.fetchone()
                if existing is not None:
                    if existing[0].strip() != digest:
                        raise RuntimeError(
                            f"적용된 dw_control migration v{version}과 파일 digest가 다릅니다"
                        )
                    applied.append((version, digest))
                    continue
                cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(OWNER_ROLE)))
                cursor.execute(source.decode("utf-8"))
                cursor.execute(
                    """INSERT INTO dw_control.schema_migrations(version, name, sha256)
                       VALUES (%s, %s, %s)""",
                    (version, name, digest),
                )
                applied.append((version, digest))
                table_exists = True
    return tuple(applied)


def main() -> None:
    with psycopg.connect(
        host=_required("POP_TALK_CONTROL_POSTGRES_HOST"),
        port=int(os.environ.get("POP_TALK_CONTROL_POSTGRES_PORT", "5432")),
        dbname=_required("POP_TALK_CONTROL_POSTGRES_DB"),
        user=_required("POP_TALK_CONTROL_ADMIN_USER"),
        password=_required("POP_TALK_CONTROL_ADMIN_PASSWORD"),
    ) as connection:
        applied = migrate(
            connection,
            runtime_password=_required("POP_TALK_CONTROL_POSTGRES_PASSWORD"),
            migration_paths=_migration_paths(),
        )
    print(
        "dw_control migration 적용 확인: "
        + ", ".join(f"v{version}={digest}" for version, digest in applied)
    )


if __name__ == "__main__":
    main()
