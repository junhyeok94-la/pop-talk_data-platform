"""S3 legacy, PostgreSQL service, Snowflake Gold 영화 집합을 읽기 전용 대사한다."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

BUCKET = "amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an"
SAFE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class ReconciliationRuntimeError(RuntimeError):
    """외부 예외의 비밀값을 출력하지 않기 위한 단계 기반 오류."""

    def __init__(self, stage: str, code: str):
        super().__init__(f"{stage}:{code}")
        self.stage = stage
        self.code = code


def _airflow_connection(conn_id: str) -> dict[str, Any]:
    """Task SDK 밖에서도 메타DB의 암호화된 연결을 서버 내부에서만 복호화한다."""
    try:
        from airflow.models.connection import Connection
        from airflow.utils.session import create_session

        with create_session() as session:
            connection = session.query(Connection).filter(Connection.conn_id == conn_id).one()
            return {
                "login": connection.login,
                "password": connection.password,
                "host": connection.host,
                "schema": connection.schema,
                "port": connection.port,
                "extra": dict(connection.extra_dejson),
            }
    except Exception:
        raise ReconciliationRuntimeError("airflow_connection", "NOT_AVAILABLE") from None


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only movie baseline reconciliation")
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--service-schema", default="dev")
    parser.add_argument(
        "--output",
        default="/opt/airflow/workbench/data-modeling/movie-baseline-reconciliation.json",
    )
    return parser.parse_args()


def _require(condition: bool, stage: str, code: str) -> None:
    if not condition:
        raise ReconciliationRuntimeError(stage, code)


def _load_legacy(bucket: str, manifest_key: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        import boto3

        airflow_connection = _airflow_connection("pop_talk_aws")
        client = boto3.client(
            "s3",
            aws_access_key_id=airflow_connection["login"],
            aws_secret_access_key=airflow_connection["password"],
            region_name=airflow_connection["extra"].get("region_name", "ap-northeast-2"),
        )
        return _load_legacy_from_client(client, bucket, manifest_key)
    except ReconciliationRuntimeError:
        raise
    except Exception:
        raise ReconciliationRuntimeError("s3_manifest", "READ_FAILED") from None


def _load_legacy_from_client(
    client: Any, bucket: str, manifest_key: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        manifest_bytes = client.get_object(Bucket=bucket, Key=manifest_key)["Body"].read()
        manifest = json.loads(manifest_bytes)
    except Exception:
        raise ReconciliationRuntimeError("s3_manifest", "READ_FAILED") from None
    try:
        from pipelines.modeling.movie_baseline_reconciliation import (
            SNAPSHOT_ERROR_CODES,
            SnapshotContractError,
            validate_legacy_manifest_header,
            validate_legacy_snapshot,
        )
        _require(isinstance(manifest, dict), "s3_manifest", "INVALID_SHAPE")
        validate_legacy_manifest_header(manifest, expected_bucket=bucket)
    except SnapshotContractError as error:
        code = str(error) if str(error) in SNAPSHOT_ERROR_CODES else "CONTRACT_FAILED"
        raise ReconciliationRuntimeError("s3_manifest", code) from None
    source_key = manifest["key"]
    try:
        source_bytes = client.get_object(Bucket=bucket, Key=source_key)["Body"].read()
    except Exception:
        raise ReconciliationRuntimeError("s3_source", "READ_FAILED") from None
    try:
        rows = validate_legacy_snapshot(manifest, source_bytes, expected_bucket=bucket)
    except SnapshotContractError as error:
        code = str(error) if str(error) in SNAPSHOT_ERROR_CODES else "CONTRACT_FAILED"
        raise ReconciliationRuntimeError("s3_source", code) from None
    source_sha = hashlib.sha256(source_bytes).hexdigest()
    return rows, {
        "bucket": bucket,
        "manifest_key": manifest_key,
        "source_key": manifest["key"],
        "source_sha256": source_sha,
        "record_count": len(rows),
    }


def _load_service(schema: str) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    _require(bool(SAFE_IDENTIFIER.fullmatch(schema)), "postgres", "INVALID_SCHEMA")
    try:
        import psycopg
        from psycopg.rows import dict_row

        connection = psycopg.connect(
            host=os.environ["POP_TALK_POSTGRES_HOST"],
            port=int(os.environ.get("POP_TALK_POSTGRES_PORT", "5432")),
            dbname=os.environ["POP_TALK_POSTGRES_DB"],
            user=os.environ["POP_TALK_POSTGRES_USER"],
            password=os.environ["POP_TALK_POSTGRES_PASSWORD"],
            row_factory=dict_row,
        )
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute(
                    f"""SELECT m.id, m.kofic_movie_cd, m.kmdb_id, m.kmdb_matched,
                               m.title_ko, m.title_en, m.title_original, m.release_date,
                               m.production_year, m.runtime_minutes, m.production_countries,
                               m.genres, m.directors,
                               m.production_companies, m.plot, m.poster_url, m.service_status::text,
                               m.approval_status::text, m.source_hash, m.source_system,
                               COALESCE(e.is_removed, FALSE) AS is_removed
                          FROM {schema}.popcorn_movies m
                          LEFT JOIN {schema}.movie_editorial e ON e.movie_id = m.id
                         ORDER BY m.id"""
                )
                movies = list(cursor.fetchall())
                relations: dict[str, list[dict[str, Any]]] = {}
                for table in (
                    "movie_editorial", "popcorn_movie_media", "movie_category_links",
                    "popcorn_movie_embeddings",
                ):
                    cursor.execute(f"SELECT movie_id FROM {schema}.{table} ORDER BY movie_id")
                    relations[table] = list(cursor.fetchall())
                cursor.execute("SELECT current_database() AS database_name, clock_timestamp() AS extracted_at")
                metadata = dict(cursor.fetchone())
    except ReconciliationRuntimeError:
        raise
    except Exception:
        raise ReconciliationRuntimeError("postgres", "READ_FAILED") from None
    metadata["schema"] = schema
    metadata["extracted_at"] = metadata["extracted_at"].isoformat()
    return movies, relations, metadata


def _load_gold() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        import snowflake.connector

        airflow_connection = _airflow_connection("pop_talk_snowflake")
        extra = airflow_connection["extra"]
        parameters = {
            "account": extra["account"],
            "user": airflow_connection["login"],
            "role": extra["role"],
            "warehouse": extra["warehouse"],
            "database": extra["database"],
            "schema": airflow_connection["schema"] or "STAGING",
            "session_parameters": extra.get("session_parameters", {}),
        }
        if extra.get("private_key_file"):
            parameters["private_key_file"] = extra["private_key_file"]
        elif airflow_connection["password"]:
            parameters["password"] = airflow_connection["password"]
        else:
            raise ReconciliationRuntimeError("snowflake", "AUTH_NOT_CONFIGURED")
        connection = snowflake.connector.connect(**parameters)
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT KOFIC_MOVIE_CD, SOURCE_RUN_ID, ARTIFACT_VERSION,
                          PUBLICATION_REVISION, EXCHANGE_READY_KEY,
                          CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_TIMESTAMP()
                     FROM POP_TALK_DW_DEV.DW.DIM_MOVIE
                    ORDER BY MOVIE_KEY"""
            )
            rows = cursor.fetchall()
            _require(bool(rows), "snowflake", "EMPTY_GOLD")
            account, database, extracted_at = rows[0][5:8]
            _require(database == extra["database"], "snowflake", "DATABASE_MISMATCH")
            _require(all(row[5:8] == rows[0][5:8] for row in rows),
                     "snowflake", "STATEMENT_METADATA_MISMATCH")
            movies = [{"kofic_movie_cd": row[0]} for row in rows]
            lineage = sorted({
                (row[1], row[2], int(row[3]), row[4]) for row in rows
            })
        connection.close()
    except ReconciliationRuntimeError:
        raise
    except Exception:
        raise ReconciliationRuntimeError("snowflake", "READ_FAILED") from None
    return movies, {
        "account": account,
        "database": database,
        "extracted_at": extracted_at.isoformat(),
        "selected_movie_source_lineage": [
            {
                "source_run_id": row[0], "artifact_version": row[1],
                "publication_revision": row[2], "exchange_ready_key": row[3],
            }
            for row in lineage
        ],
    }


def main() -> int:
    args = _arguments()
    try:
        from pipelines.modeling.movie_baseline_reconciliation import (
            RULE_VERSION,
            reconcile_movies,
            relation_impact_counts,
        )

        legacy_rows, legacy_input = _load_legacy(args.bucket, args.manifest_key)
        service_rows, relations, postgres_input = _load_service(args.service_schema)
        gold_rows, gold_input = _load_gold()
        result = reconcile_movies(legacy_rows, service_rows, gold_rows)
        result["inputs"] = {
            "legacy": legacy_input,
            "postgres": postgres_input,
            "gold": gold_input,
            "extracted_by_rule_version": RULE_VERSION,
            "completed_at": datetime.now(UTC).isoformat(),
        }
        result["service_relation_impact"] = {
            table: relation_impact_counts(service_rows, rows)
            for table, rows in sorted(relations.items())
        }
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
                             encoding="utf-8")
        temporary.replace(output)
        print(json.dumps({
            "status": "SUCCESS",
            "rule_version": result["rule_version"],
            "legacy_rows": result["legacy"]["total_rows"],
            "service_rows": result["service"]["total_rows"],
            "gold_rows": result["gold"]["total_rows"],
            "output": str(output),
        }, ensure_ascii=False, sort_keys=True))
        return 0
    except ReconciliationRuntimeError as error:
        print(json.dumps({"status": "FAILED", "stage": error.stage, "code": error.code},
                         sort_keys=True), file=sys.stderr)
        return 2
    except Exception:
        print(json.dumps({"status": "FAILED", "stage": "internal", "code": "UNEXPECTED"},
                         sort_keys=True), file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
