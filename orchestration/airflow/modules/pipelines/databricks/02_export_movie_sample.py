# Databricks notebook source
# MAGIC %md
# MAGIC # Pop Talk movie sample: Silver -> exchange JSONL
# MAGIC
# MAGIC Exports one validated MVP run as a single JSON Lines file for manual
# MAGIC transfer to Snowflake. Single-file collection is intentionally limited
# MAGIC to small samples; production-sized exports should use distributed Parquet.

# COMMAND ----------

import hashlib
import json

from pyspark.sql import functions as F

dbutils.widgets.text("run_id", "movie-sample-20260909T003725Z", "Run ID")
run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("run_id must not be empty")

expected_count = 100
exchange_dir = f"/Volumes/workspace/silver/exchange/{run_id}"
data_path = f"{exchange_dir}/movies_silver.jsonl"
manifest_path = f"{exchange_dir}/export_manifest.json"

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.silver.exchange
COMMENT 'Validated files exported from Silver for downstream loading'
""")

silver_rows = (
    spark.table("workspace.silver.movies")
    .where(F.col("run_id") == run_id)
    .orderBy("source_movie_id")
)

actual_count = silver_rows.count()
distinct_count = silver_rows.select("source_movie_id").distinct().count()

if actual_count != expected_count:
    raise ValueError(
        f"Expected {expected_count} Silver rows for {run_id}, got {actual_count}"
    )

if distinct_count != actual_count:
    raise ValueError(
        f"Duplicate movie IDs detected: rows={actual_count}, distinct={distinct_count}"
    )

# COMMAND ----------

# This collection is acceptable for the 100-row MVP only. Do not use it for the
# full dataset; Spark should write larger exports as partitioned Parquet files.
json_lines = [
    row["json_value"]
    for row in silver_rows.select(
        F.to_json(F.struct(*[F.col(name) for name in silver_rows.columns])).alias(
            "json_value"
        )
    ).collect()
]

payload = ("\n".join(json_lines) + "\n").encode("utf-8")
payload_sha256 = hashlib.sha256(payload).hexdigest()

dbutils.fs.mkdirs(exchange_dir)
with open(data_path, "wb") as data_stream:
    data_stream.write(payload)

export_manifest = {
    "manifest_version": 1,
    "run_id": run_id,
    "source_table": "workspace.silver.movies",
    "target_system": "snowflake",
    "record_count": actual_count,
    "distinct_movie_count": distinct_count,
    "data_file": {
        "name": "movies_silver.jsonl",
        "format": "json_lines",
        "encoding": "utf-8",
        "bytes": len(payload),
        "sha256": payload_sha256,
    },
}

manifest_payload = json.dumps(
    export_manifest,
    ensure_ascii=False,
    indent=2,
).encode("utf-8")

with open(manifest_path, "wb") as manifest_stream:
    manifest_stream.write(manifest_payload)

print(f"record_count={actual_count}")
print(f"data_path={data_path}")
print(f"manifest_path={manifest_path}")
print(f"sha256={payload_sha256}")

# COMMAND ----------

display(
    silver_rows.select(
        "source_movie_id",
        "kofic_movie_cd",
        "kmdb_id",
        "title_ko",
        "release_date",
        "genres",
        "directors",
    ).limit(10)
)
