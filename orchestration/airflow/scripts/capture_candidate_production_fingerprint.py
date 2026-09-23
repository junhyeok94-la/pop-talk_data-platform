"""Legacy candidate 실행 전후의 production 경계를 읽기 전용 fingerprint한다."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


class FingerprintError(RuntimeError):
    """비밀이나 외부 응답 본문을 포함하지 않는 단계 오류."""


def canonical(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [canonical(item) for item in value]
    return value


def digest(value: Any) -> str:
    body = json.dumps(
        canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def airflow_connection(conn_id: str) -> dict[str, Any]:
    try:
        from airflow.models.connection import Connection
        from airflow.utils.session import create_session

        with create_session() as session:
            connection = session.query(Connection).filter(Connection.conn_id == conn_id).one()
            return {
                "host": connection.host,
                "login": connection.login,
                "password": connection.password,
                "schema": connection.schema,
                "port": connection.port,
                "extra": dict(connection.extra_dejson),
            }
    except Exception:
        raise FingerprintError(f"connection:{conn_id}:NOT_AVAILABLE") from None


def s3_fingerprint() -> dict[str, Any]:
    try:
        import boto3

        connection = airflow_connection("pop_talk_aws")
        client = boto3.client(
            "s3",
            aws_access_key_id=connection["login"],
            aws_secret_access_key=connection["password"],
            region_name=connection["extra"].get("region_name", "ap-northeast-2"),
        )
        bucket = "amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an"
        prefixes = (
            "raw/movie_api_daily/v1/",
            "manifests/movie_api_daily/v1/",
            "exchange/movie_silver/v1/",
            "raw/legacy_snapshot/v1/",
            "manifests/legacy_snapshot/v1/",
        )
        result = {}
        paginator = client.get_paginator("list_objects_v2")
        for prefix in prefixes:
            rows = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for item in page.get("Contents") or []:
                    head = client.head_object(Bucket=bucket, Key=item["Key"])
                    rows.append({
                        "key": item["Key"],
                        "size": int(item["Size"]),
                        "etag": str(item.get("ETag") or "").strip('"'),
                        "metadata_sha256": (head.get("Metadata") or {}).get("sha256"),
                        "version_id": head.get("VersionId"),
                    })
            rows.sort(key=lambda row: row["key"])
            result[prefix] = {
                "object_count": len(rows),
                "total_bytes": sum(row["size"] for row in rows),
                "digest": digest(rows),
            }
        return {"bucket": bucket, "prefixes": result}
    except FingerprintError:
        raise
    except Exception:
        raise FingerprintError("s3:READ_FAILED") from None


def _rows(cursor: Any, statement: str) -> list[dict[str, Any]]:
    cursor.execute(statement)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def snowflake_fingerprint() -> dict[str, Any]:
    try:
        import snowflake.connector

        airflow = airflow_connection("pop_talk_snowflake")
        extra = airflow["extra"]
        parameters = {
            "account": extra["account"], "user": airflow["login"],
            "role": extra["role"], "warehouse": extra["warehouse"],
            "database": extra["database"], "schema": airflow["schema"] or "STAGING",
            "session_parameters": extra.get("session_parameters", {}),
        }
        if extra.get("private_key_file"):
            parameters["private_key_file"] = extra["private_key_file"]
        elif airflow["password"]:
            parameters["password"] = airflow["password"]
        connection = snowflake.connector.connect(**parameters)
        queries = {
            "staging_movie": """SELECT SOURCE_RUN_ID,ARTIFACT_VERSION,CANONICAL_MOVIE_KEY,
                RECORD_SHA256,EXCHANGE_READY_KEY FROM POP_TALK_DW_DEV.STAGING.MOVIE_OBSERVATIONS_RAW
                ORDER BY SOURCE_RUN_ID,ARTIFACT_VERSION,CANONICAL_MOVIE_KEY""",
            "staging_boxoffice": """SELECT SOURCE_RUN_ID,ARTIFACT_VERSION,TARGET_DATE,
                KOFIC_MOVIE_CD,RECORD_SHA256,EXCHANGE_READY_KEY
                FROM POP_TALK_DW_DEV.STAGING.BOXOFFICE_OBSERVATIONS_RAW
                ORDER BY SOURCE_RUN_ID,ARTIFACT_VERSION,TARGET_DATE,KOFIC_MOVIE_CD""",
            "staging_loads": """SELECT EXCHANGE_READY_KEY,SOURCE_RUN_ID,ARTIFACT_VERSION,
                PUBLICATION_REVISION,SOURCE_BUNDLE_SHA256,STATUS,MOVIE_COUNT,BOXOFFICE_COUNT
                FROM POP_TALK_DW_DEV.STAGING.SILVER_EXCHANGE_LOADS ORDER BY EXCHANGE_READY_KEY""",
            "gold_movie": "SELECT * FROM POP_TALK_DW_DEV.DW.DIM_MOVIE ORDER BY MOVIE_KEY",
            "gold_boxoffice": "SELECT * FROM POP_TALK_DW_DEV.DW.FCT_BOXOFFICE_DAILY ORDER BY BOXOFFICE_KEY",
            "gold_quality": "SELECT * FROM POP_TALK_DW_DEV.MART.MART_DATA_QUALITY ORDER BY SOURCE_RUN_ID,ARTIFACT_VERSION",
        }
        result = {}
        with connection.cursor() as cursor:
            for name, statement in queries.items():
                rows = _rows(cursor, statement)
                result[name] = {"row_count": len(rows), "digest": digest(rows)}
            cursor.execute("SELECT CURRENT_ACCOUNT(),CURRENT_DATABASE(),CURRENT_TIMESTAMP()")
            account, database, captured_at = cursor.fetchone()
        connection.close()
        return {
            "account": account,
            "database": database,
            "captured_at": captured_at.isoformat(),
            "datasets": result,
        }
    except FingerprintError:
        raise
    except Exception:
        raise FingerprintError("snowflake:READ_FAILED") from None


def postgres_fingerprint() -> dict[str, Any]:
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
                cursor.execute("""SELECT a.publication_id,p.generation,p.model_version,
                    p.snapshot_sha256,p.movie_count,p.boxoffice_count,p.quality_count
                    FROM dw_serving.active_publications_v3 a
                    JOIN dw_serving.dataset_publications_v3 p USING(publication_id)
                    WHERE a.dataset_name='movie_gold'""")
                publication = cursor.fetchone()
                if not publication:
                    raise FingerprintError("postgres:NO_ACTIVE_PUBLICATION")
                publication_id = publication["publication_id"]
                cursor.execute("""SELECT movie_key,row_sha256
                    FROM dw_serving.movie_snapshot_v3 WHERE publication_id=%s
                    ORDER BY movie_key""", (publication_id,))
                movies = list(cursor.fetchall())
                cursor.execute("""SELECT boxoffice_key,row_sha256
                    FROM dw_serving.boxoffice_snapshot_v3 WHERE publication_id=%s
                    ORDER BY boxoffice_key""", (publication_id,))
                boxoffice = list(cursor.fetchall())
        return {
            "active_publication": canonical(dict(publication)),
            "movies": {"row_count": len(movies), "digest": digest(movies)},
            "boxoffice": {"row_count": len(boxoffice), "digest": digest(boxoffice)},
        }
    except FingerprintError:
        raise
    except Exception:
        raise FingerprintError("postgres:READ_FAILED") from None


def _databricks_statement(host: str, token: str, warehouse_id: str, statement: str) -> list[dict]:
    import requests

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        host.rstrip("/") + "/api/2.0/sql/statements",
        headers=headers,
        json={"warehouse_id": warehouse_id, "statement": statement,
              "wait_timeout": "30s", "disposition": "INLINE"},
        timeout=(10, 45),
    )
    if response.status_code not in (200, 201):
        raise FingerprintError(f"databricks:STATEMENT_HTTP_{response.status_code}")
    payload = response.json()
    statement_id = payload.get("statement_id")
    for _ in range(12):
        state = (payload.get("status") or {}).get("state")
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELED", "CLOSED"}:
            raise FingerprintError(f"databricks:STATEMENT_{state}")
        time.sleep(2)
        polled = requests.get(
            host.rstrip("/") + f"/api/2.0/sql/statements/{statement_id}",
            headers=headers, timeout=(10, 30),
        )
        if polled.status_code != 200:
            raise FingerprintError(f"databricks:POLL_HTTP_{polled.status_code}")
        payload = polled.json()
    else:
        raise FingerprintError("databricks:STATEMENT_TIMEOUT")
    columns = [column["name"] for column in payload["manifest"]["schema"]["columns"]]
    data = (payload.get("result") or {}).get("data_array") or []
    if (payload.get("manifest") or {}).get("total_chunk_count", 1) != 1:
        raise FingerprintError("databricks:MULTI_CHUNK_NOT_SUPPORTED")
    return [dict(zip(columns, row)) for row in data]


def _databricks_jobs_fingerprint(host: str, token: str) -> dict[str, Any]:
    """SQL Warehouse API가 제한된 Free Edition에서 serverless Job으로 읽기만 한다."""
    import base64
    import requests

    project_root = Path(__file__).resolve().parents[1] / "modules"
    notebook = (project_root / "pipelines/databricks/05_production_fingerprint.py").read_bytes()
    version = hashlib.sha256(notebook).hexdigest()
    directory = "/Shared/pop-talk/production_fingerprint_versions"
    notebook_path = f"{directory}/{version}"
    headers = {"Authorization": f"Bearer {token}"}
    mkdir = requests.post(
        host.rstrip("/") + "/api/2.0/workspace/mkdirs",
        headers=headers, json={"path": directory}, timeout=(10, 60),
    )
    if mkdir.status_code not in (200, 201):
        raise FingerprintError(f"databricks:JOB_MKDIR_HTTP_{mkdir.status_code}")
    status = requests.get(
        host.rstrip("/") + "/api/2.0/workspace/get-status",
        headers=headers, params={"path": notebook_path}, timeout=(10, 60),
    )
    if status.status_code == 200:
        exported = requests.get(
            host.rstrip("/") + "/api/2.0/workspace/export",
            headers=headers, params={"path": notebook_path, "format": "SOURCE"},
            timeout=(10, 60),
        )
        try:
            remote = base64.b64decode(exported.json()["content"], validate=True)
        except Exception:
            raise FingerprintError("databricks:JOB_NOTEBOOK_EXPORT_INVALID") from None
        if exported.status_code != 200 or remote != notebook:
            raise FingerprintError("databricks:JOB_NOTEBOOK_CONFLICT")
    elif status.status_code == 404:
        imported = requests.post(
            host.rstrip("/") + "/api/2.0/workspace/import",
            headers=headers,
            json={
                "path": notebook_path, "format": "SOURCE", "language": "PYTHON",
                "content": base64.b64encode(notebook).decode("ascii"), "overwrite": False,
            },
            timeout=(10, 60),
        )
        if imported.status_code not in (200, 201):
            raise FingerprintError(f"databricks:JOB_IMPORT_HTTP_{imported.status_code}")
    else:
        raise FingerprintError(f"databricks:JOB_STATUS_HTTP_{status.status_code}")

    submitted = requests.post(
        host.rstrip("/") + "/api/2.1/jobs/runs/submit",
        headers=headers,
        json={
            "run_name": f"pop-talk-production-fingerprint-{version[:12]}",
            "performance_target": "STANDARD",
            "tasks": [{
                "task_key": "capture_production_fingerprint",
                "notebook_task": {"notebook_path": notebook_path, "source": "WORKSPACE"},
                "timeout_seconds": 1800,
            }],
        },
        timeout=(10, 60),
    )
    if submitted.status_code not in (200, 201):
        raise FingerprintError(f"databricks:JOB_SUBMIT_HTTP_{submitted.status_code}")
    run_id = submitted.json().get("run_id")
    task_run_id = None
    for _ in range(120):
        status = requests.get(
            host.rstrip("/") + "/api/2.2/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id, "include_history": "true"},
            timeout=(10, 30),
        )
        if status.status_code != 200:
            raise FingerprintError(f"databricks:JOB_POLL_HTTP_{status.status_code}")
        payload = status.json()
        tasks = payload.get("tasks") or []
        if tasks:
            successful = [
                task for task in tasks
                if (task.get("state") or {}).get("result_state") == "SUCCESS"
            ]
            task_run_id = (successful or tasks)[-1].get("run_id")
        life_cycle = (payload.get("state") or {}).get("life_cycle_state")
        if life_cycle == "TERMINATED":
            result_state = (payload.get("state") or {}).get("result_state")
            if result_state != "SUCCESS":
                raise FingerprintError(f"databricks:JOB_{result_state or 'FAILED'}")
            break
        if life_cycle in {"SKIPPED", "INTERNAL_ERROR"}:
            raise FingerprintError(f"databricks:JOB_{life_cycle}")
        time.sleep(5)
    else:
        raise FingerprintError("databricks:JOB_TIMEOUT")
    if not task_run_id:
        raise FingerprintError("databricks:JOB_TASK_RUN_MISSING")
    output = requests.get(
        host.rstrip("/") + "/api/2.1/jobs/runs/get-output",
        headers=headers, params={"run_id": task_run_id}, timeout=(10, 60),
    )
    if output.status_code != 200:
        raise FingerprintError(f"databricks:JOB_OUTPUT_HTTP_{output.status_code}")
    try:
        result = json.loads(output.json()["notebook_output"]["result"])
    except Exception:
        raise FingerprintError("databricks:JOB_OUTPUT_INVALID") from None
    if not isinstance(result, dict) or set(result) != {"datasets", "latest_history"}:
        raise FingerprintError("databricks:JOB_OUTPUT_CONTRACT")
    return result


def databricks_fingerprint() -> dict[str, Any]:
    try:
        import requests

        connection = airflow_connection("pop_talk_databricks")
        host, token = connection["host"], connection["password"]
        response = requests.get(
            host.rstrip("/") + "/api/2.0/sql/warehouses",
            headers={"Authorization": f"Bearer {token}"}, timeout=(10, 30),
        )
        if response.status_code == 403:
            result = _databricks_jobs_fingerprint(host, token)
            return {
                "workspace_id": "7474644394100716",
                "warehouse_name": None,
                "fallback": "serverless_job_read_only",
                **result,
            }
        if response.status_code != 200:
            raise FingerprintError(f"databricks:WAREHOUSE_HTTP_{response.status_code}")
        warehouses = response.json().get("warehouses") or []
        if not warehouses:
            raise FingerprintError("databricks:NO_SQL_WAREHOUSE")
        warehouse = next((item for item in warehouses if item.get("state") == "RUNNING"), warehouses[0])
        warehouse_id = warehouse["id"]
        queries = {
            "bronze_raw": """SELECT object_key,payload_sha256 FROM workspace.bronze.movie_raw_objects
                ORDER BY object_key""",
            "silver_movie_observations": """SELECT source_run_id,transform_version,
                canonical_movie_key,content_sha256,policy_sha256,matching_sha256
                FROM workspace.silver.movie_observations
                ORDER BY source_run_id,transform_version,canonical_movie_key""",
            "silver_movies_current": """SELECT canonical_movie_key,content_sha256,policy_sha256,
                matching_sha256 FROM workspace.silver.movies_current ORDER BY canonical_movie_key""",
            "silver_boxoffice_observations": """SELECT source_run_id,transform_version,target_date,
                kofic_movie_cd,observation_sha256 FROM workspace.silver.boxoffice_observations
                ORDER BY source_run_id,transform_version,target_date,kofic_movie_cd""",
            "silver_boxoffice_current": """SELECT target_date,kofic_movie_cd,observation_sha256
                FROM workspace.silver.boxoffice_current ORDER BY target_date,kofic_movie_cd""",
            "silver_runs": """SELECT ready_manifest_key,source_run_id,transform_version,
                publication_revision,processing_attempt,status,bronze_object_count,movie_count,
                boxoffice_row_count FROM workspace.silver.movie_transform_runs
                ORDER BY ready_manifest_key,transform_version,publication_revision,processing_attempt""",
        }
        datasets = {}
        for name, statement in queries.items():
            rows = _databricks_statement(host, token, warehouse_id, statement)
            datasets[name] = {"row_count": len(rows), "digest": digest(rows)}
        histories = {}
        for table in (
            "workspace.bronze.movie_raw_objects",
            "workspace.silver.movie_observations",
            "workspace.silver.movies_current",
            "workspace.silver.boxoffice_observations",
            "workspace.silver.boxoffice_current",
            "workspace.silver.movie_transform_runs",
        ):
            rows = _databricks_statement(host, token, warehouse_id, f"DESCRIBE HISTORY {table} LIMIT 1")
            histories[table] = canonical(rows[0] if rows else None)
        return {
            "workspace_id": "7474644394100716",
            "warehouse_name": warehouse.get("name"),
            "datasets": datasets,
            "latest_history": histories,
        }
    except FingerprintError:
        raise
    except Exception:
        raise FingerprintError("databricks:READ_FAILED") from None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    try:
        result = {
            "schema_version": 1,
            "purpose": "legacy_candidate_production_boundary_fingerprint",
            "captured_at": datetime.now().astimezone().isoformat(),
            "s3": s3_fingerprint(),
            "databricks": databricks_fingerprint(),
            "snowflake": snowflake_fingerprint(),
            "postgres": postgres_fingerprint(),
        }
        output = Path(arguments.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(
            json.dumps(canonical(result), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(output)
        print(json.dumps({
            "status": "SUCCESS",
            "output": str(output),
            "s3_prefixes": len(result["s3"]["prefixes"]),
            "databricks_datasets": len(result["databricks"]["datasets"]),
            "snowflake_datasets": len(result["snowflake"]["datasets"]),
            "postgres_publication": result["postgres"]["active_publication"]["publication_id"],
        }, sort_keys=True))
        return 0
    except FingerprintError as error:
        print(json.dumps({"status": "FAILED", "code": str(error)}), file=sys.stderr)
        return 2
    except Exception:
        print(json.dumps({"status": "FAILED", "code": "internal:UNEXPECTED"}), file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
