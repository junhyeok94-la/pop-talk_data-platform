# Databricks notebook source
# MAGIC %md
# MAGIC # Pop Talk production Delta read-only fingerprint
# MAGIC
# MAGIC Candidate 실행 전후 격리 검증용이다. 운영 Delta를 읽기만 하며 테이블·파일을 쓰지 않는다.

# COMMAND ----------

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal


def normalize(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): normalize(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    return value


def fingerprint(statement):
    rows = [normalize(row.asDict(recursive=True)) for row in spark.sql(statement).collect()]
    body = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {"row_count": len(rows), "digest": hashlib.sha256(body.encode("utf-8")).hexdigest()}


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

datasets = {name: fingerprint(statement) for name, statement in queries.items()}
history = {}
for table in (
    "workspace.bronze.movie_raw_objects",
    "workspace.silver.movie_observations",
    "workspace.silver.boxoffice_observations",
    "workspace.silver.movie_transform_runs",
):
    rows = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select(
        "version", "operation", "operationParameters", "userMetadata"
    ).collect()
    history[table] = normalize(rows[0].asDict(recursive=True) if rows else None)

dbutils.notebook.exit(json.dumps({
    "datasets": datasets,
    "latest_history": history,
}, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
