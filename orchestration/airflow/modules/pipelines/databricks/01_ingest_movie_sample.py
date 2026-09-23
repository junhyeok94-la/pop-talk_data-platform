# Databricks notebook source
# MAGIC %md
# MAGIC # Pop Talk movie sample: landing -> Bronze -> Silver
# MAGIC
# MAGIC Validates the uploaded manifest and file checksum, preserves each input
# MAGIC JSON line in Bronze, and idempotently merges cleaned movie rows into Silver.

# COMMAND ----------

import hashlib
import json

from pyspark.sql import functions as F
from pyspark.sql.types import LongType

dbutils.widgets.text("run_id", "movie-sample-20260909T003725Z", "Run ID")
run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("run_id must not be empty")

landing_dir = f"/Volumes/workspace/bronze/landing/{run_id}"
data_path = f"{landing_dir}/movies.jsonl"
manifest_path = f"{landing_dir}/manifest.json"

print(f"run_id={run_id}")
print(f"data_path={data_path}")

# COMMAND ----------

with open(manifest_path, "r", encoding="utf-8") as manifest_stream:
    manifest = json.load(manifest_stream)

with open(data_path, "rb") as data_stream:
    actual_sha256 = hashlib.sha256(data_stream.read()).hexdigest()

expected_sha256 = manifest["data_file"]["sha256"]
expected_count = int(manifest["selection"]["actual_count"])

if manifest["run_id"] != run_id:
    raise ValueError(
        f"Manifest run_id mismatch: expected {run_id}, got {manifest['run_id']}"
    )

if actual_sha256 != expected_sha256:
    raise ValueError(
        f"SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
    )

print(f"manifest source={manifest['source']}")
print(f"manifest expected_count={expected_count}")
print(f"sha256={actual_sha256}")

# COMMAND ----------

landing_lines = spark.read.text(data_path)
actual_count = landing_lines.count()

if actual_count != expected_count:
    raise ValueError(
        f"Record count mismatch: expected {expected_count}, got {actual_count}"
    )

bronze_rows = (
    landing_lines
    .select(
        F.lit(run_id).alias("run_id"),
        F.lit(manifest["source"]).alias("source"),
        F.lit(data_path).alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
        F.get_json_object("value", "$.id").cast(LongType()).alias("record_id"),
        F.col("value").alias("payload_json"),
        F.sha2("value", 256).alias("payload_sha256"),
    )
)

invalid_record_count = bronze_rows.where(F.col("record_id").isNull()).count()
if invalid_record_count:
    raise ValueError(f"Found {invalid_record_count} rows without a valid numeric id")

bronze_rows.createOrReplaceTempView("incoming_bronze_movies")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.bronze.movies_raw (
    run_id STRING,
    source STRING,
    source_file STRING,
    ingested_at TIMESTAMP,
    record_id BIGINT,
    payload_json STRING,
    payload_sha256 STRING
) USING DELTA
""")

spark.sql("""
MERGE INTO workspace.bronze.movies_raw AS target
USING incoming_bronze_movies AS source
ON target.run_id = source.run_id AND target.record_id = source.record_id
WHEN MATCHED AND target.payload_sha256 <> source.payload_sha256 THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

bronze_count = spark.sql(f"""
SELECT COUNT(*) AS count
FROM workspace.bronze.movies_raw
WHERE run_id = '{run_id}'
""").first()["count"]

if bronze_count != expected_count:
    raise ValueError(
        f"Bronze count mismatch: expected {expected_count}, got {bronze_count}"
    )

print(f"bronze_count={bronze_count}")

# COMMAND ----------

movie_schema = spark.read.json(data_path).schema

parsed_movies = (
    spark.table("workspace.bronze.movies_raw")
    .where(F.col("run_id") == run_id)
    .select(
        "run_id",
        "source",
        "ingested_at",
        "payload_sha256",
        F.from_json("payload_json", movie_schema).alias("movie"),
    )
    .select("run_id", "source", "ingested_at", "payload_sha256", "movie.*")
)

silver_movies = parsed_movies.select(
    F.col("id").cast("long").alias("source_movie_id"),
    F.trim("kofic_movie_cd").alias("kofic_movie_cd"),
    F.trim("kmdb_id").alias("kmdb_id"),
    F.col("kmdb_matched").cast("boolean").alias("kmdb_matched"),
    F.trim(F.regexp_replace("title_ko", r"\\s+", " ")).alias("title_ko"),
    F.trim(F.regexp_replace("title_en", r"\\s+", " ")).alias("title_en"),
    F.trim(F.regexp_replace("title_original", r"\\s+", " ")).alias("title_original"),
    F.to_date("release_date").alias("release_date"),
    F.col("production_year").cast("int").alias("production_year"),
    F.col("runtime_minutes").cast("int").alias("runtime_minutes"),
    "movie_type",
    "production_status",
    "production_countries",
    "representative_country",
    "genres",
    "representative_genre",
    "directors",
    "director_names_en",
    "actors",
    "actor_roles",
    "production_companies",
    "viewing_grade",
    "poster_url",
    "plot",
    "source_keywords",
    "service_status",
    "approval_status",
    "approved_by",
    F.to_timestamp("approved_at").alias("approved_at"),
    "rejection_reason",
    "source_system",
    F.to_timestamp("source_synced_at").alias("source_synced_at"),
    F.to_timestamp("created_at").alias("source_created_at"),
    F.to_timestamp("updated_at").alias("source_updated_at"),
    "categories",
    "run_id",
    F.col("source").alias("ingestion_source"),
    "ingested_at",
    F.current_timestamp().alias("processed_at"),
    F.col("payload_sha256").alias("record_sha256"),
)

silver_movies.createOrReplaceTempView("incoming_silver_movies")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.movies
USING DELTA
AS SELECT * FROM incoming_silver_movies WHERE 1 = 0
""")

spark.sql("""
MERGE INTO workspace.silver.movies AS target
USING incoming_silver_movies AS source
ON target.ingestion_source = source.ingestion_source
   AND target.source_movie_id = source.source_movie_id
WHEN MATCHED AND target.record_sha256 <> source.record_sha256 THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

silver_count_for_run = spark.sql(f"""
SELECT COUNT(*) AS count
FROM workspace.silver.movies
WHERE run_id = '{run_id}'
""").first()["count"]

if silver_count_for_run != expected_count:
    raise ValueError(
        f"Silver count mismatch: expected {expected_count}, got {silver_count_for_run}"
    )

print(f"silver_count_for_run={silver_count_for_run}")

# COMMAND ----------

display(spark.sql(f"""
SELECT
    run_id,
    COUNT(*) AS movie_count,
    COUNT(DISTINCT source_movie_id) AS distinct_movie_count,
    SUM(CASE WHEN title_ko IS NULL OR title_ko = '' THEN 1 ELSE 0 END) AS missing_title_count,
    SUM(CASE WHEN release_date IS NULL THEN 1 ELSE 0 END) AS missing_release_date_count,
    MIN(release_date) AS minimum_release_date,
    MAX(release_date) AS maximum_release_date
FROM workspace.silver.movies
WHERE run_id = '{run_id}'
GROUP BY run_id
"""))

display(
    spark.table("workspace.silver.movies")
    .where(F.col("run_id") == run_id)
    .select(
        "source_movie_id",
        "kofic_movie_cd",
        "kmdb_id",
        "title_ko",
        "release_date",
        "genres",
        "directors",
    )
    .orderBy("source_movie_id")
    .limit(10)
)
