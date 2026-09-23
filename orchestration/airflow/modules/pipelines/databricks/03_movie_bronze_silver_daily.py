# Databricks notebook source
# MAGIC %md
# MAGIC # Pop Talk daily movie Raw -> Bronze/Silver
# MAGIC
# MAGIC Consumes an immutable bundle staged by Airflow. It retains raw objects,
# MAGIC appends run observations, advances current state only by source observation
# MAGIC time, and records an idempotent transform ledger.

# COMMAND ----------

import hashlib
import json
import types
import zipfile
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

dbutils.widgets.text("landing_dir", "", "Immutable landing directory")
dbutils.widgets.text("ready_manifest_key", "", "Source S3 READY key")
dbutils.widgets.text("artifact_version", "", "Processing artifact SHA-256")
dbutils.widgets.text("publication_revision", "1", "Monotonic publication revision")
dbutils.widgets.text("processing_attempt", "1", "Explicit processing attempt")
landing_dir = dbutils.widgets.get("landing_dir").rstrip("/")
ready_manifest_key = dbutils.widgets.get("ready_manifest_key").strip()
artifact_version = dbutils.widgets.get("artifact_version").strip()
publication_revision = int(dbutils.widgets.get("publication_revision"))
processing_attempt = int(dbutils.widgets.get("processing_attempt"))
if not landing_dir.startswith("/Volumes/workspace/bronze/landing/movie_api_daily/"):
    raise ValueError("Unexpected landing directory")
if not ready_manifest_key.startswith("manifests/movie_api_daily/"):
    raise ValueError("Unexpected READY manifest key")
if len(artifact_version) != 64 or any(c not in "0123456789abcdef" for c in artifact_version):
    raise ValueError("Invalid processing artifact version")
if processing_attempt < 1:
    raise ValueError("Processing attempt must be positive")
if publication_revision < 1:
    raise ValueError("Publication revision must be positive")


bundle = zipfile.ZipFile(f"{landing_dir}/bundle.zip")


def read_bytes(relative):
    return bundle.read(relative)


def read_json(relative):
    return json.loads(read_bytes(relative))


index = read_json("bundle_index.json")
if index["ready_key"] != ready_manifest_key:
    raise ValueError("Bundle/parameter READY key mismatch")
if index["artifact_version"] != artifact_version:
    raise ValueError("Bundle/parameter artifact version mismatch")
for relative, expected in index["documents"].items():
    if hashlib.sha256(read_bytes(relative)).hexdigest() != expected:
        raise ValueError(f"Bundle checksum mismatch: {relative}")

transform = types.ModuleType("pop_talk_movie_transform")
exec(compile(read_bytes("movie_bronze_silver.py"), "movie_bronze_silver.py", "exec"),
     transform.__dict__)

ready = read_json("DAILY_READY.json")
success = read_json("SUCCESS.json")
stages = {name: read_json(relative) for name, relative in index["stage_files"].items()}
raw = {key: read_json(relative) for key, relative in index["raw_index"].items()}
output = transform.transform_run(ready, success, stages, raw)
exchange = transform.exchange_observations(
    output, raw, ready_manifest_key, artifact_version
)
for export_name, rows in (
    ("silver_movies.jsonl", exchange["silver_movies"]),
    ("silver_boxoffice.jsonl", exchange["silver_boxoffice"]),
):
    expected_export = b"".join(transform.packed(row) + b"\n" for row in rows)
    if read_bytes(export_name) != expected_export:
        raise ValueError(f"Bundled Silver export differs from computed output: {export_name}")
run_id = ready["run_id"]
transform_version = artifact_version

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")


def string_field(name):
    return T.StructField(name, T.StringType(), True)


def bool_field(name):
    return T.StructField(name, T.BooleanType(), True)


def long_field(name):
    return T.StructField(name, T.LongType(), True)


bronze_schema = T.StructType([
    string_field("run_id"), string_field("object_key"), string_field("source"),
    string_field("resource"), string_field("request_json"), string_field("collected_at"),
    string_field("payload_sha256"), string_field("payload_json"),
])

array_names = ("countries", "genres", "directors", "director_names_en", "actors",
               "actor_roles", "production_companies", "posters", "stills", "keywords",
               "policy_exclusion_reasons")
string_names = (
    "canonical_movie_key", "kofic_movie_cd", "kmdb_id", "kmdb_mapping_status",
    "provisional_kmdb_id", "title_ko", "title_en", "title_original", "release_date",
    "movie_type", "production_status", "viewing_grade", "poster_url", "plot",
    "kmdb_rating", "vod_url", "policy_version", "ingestion_source", "source_run_id",
    "source_object_key", "source_service_status", "source_approval_status",
    "content_sha256", "policy_sha256", "embedding_input_sha256", "matching_sha256",
)
movie_schema = T.StructType(
    [string_field(name) for name in string_names]
    + [long_field(name) for name in ("service_movie_id", "production_year", "runtime_minutes",
                                     "kmdb_candidate_count")]
    + [bool_field(name) for name in ("kmdb_matched", "kmdb_candidates_truncated",
                                     "kmdb_candidates_ambiguous", "policy_eligible")]
    + [T.StructField(name, T.ArrayType(T.StringType()), True) for name in array_names]
)
boxoffice_schema = T.StructType([
    string_field("source_run_id"), string_field("target_date"), string_field("kofic_movie_cd"),
    long_field("rank"), long_field("sales_amount"), long_field("audience_count"),
    long_field("audience_accumulated"), string_field("source_object_key"),
    string_field("observation_sha256"),
])


def source_observed_at(row):
    return raw[row["source_object_key"]]["collected_at"]


bronze_df = spark.createDataFrame(output["bronze"], bronze_schema).withColumn(
    "ingested_at", F.current_timestamp())
movie_rows = exchange["silver_movies"]
movie_df = spark.createDataFrame(movie_rows, movie_schema
    .add(string_field("source_observed_at"))
    .add(string_field("ready_manifest_key"))
    .add(string_field("transform_version"))) \
    .withColumn("source_observed_at", F.to_timestamp("source_observed_at")) \
    .withColumn("processed_at", F.current_timestamp())
box_rows = exchange["silver_boxoffice"]
box_df = spark.createDataFrame(box_rows, boxoffice_schema
    .add(string_field("source_observed_at"))
    .add(string_field("ready_manifest_key"))
    .add(string_field("transform_version"))) \
    .withColumn("source_observed_at", F.to_timestamp("source_observed_at")) \
    .withColumn("processed_at", F.current_timestamp())

# COMMAND ----------

LEDGER = "workspace.silver.movie_transform_runs"
BRONZE = "workspace.bronze.movie_raw_objects"
OBS = "workspace.silver.movie_observations"
CURRENT = "workspace.silver.movies_current"
BOX_OBS = "workspace.silver.boxoffice_observations"
BOX_CURRENT = "workspace.silver.boxoffice_current"

# One-time schema migration from the first integration run. A separate attempt
# key prevents a failed retry from overwriting another attempt's SUCCESS.
if spark.catalog.tableExists(LEDGER) and "processing_attempt" not in spark.table(LEDGER).columns:
    spark.sql(f"ALTER TABLE {LEDGER} ADD COLUMNS (processing_attempt BIGINT)")
    spark.sql(f"UPDATE {LEDGER} SET processing_attempt = 1 WHERE processing_attempt IS NULL")
if spark.catalog.tableExists(LEDGER) and "publication_revision" not in spark.table(LEDGER).columns:
    spark.sql(f"ALTER TABLE {LEDGER} ADD COLUMNS (publication_revision BIGINT)")
    spark.sql(f"UPDATE {LEDGER} SET publication_revision = 1 WHERE publication_revision IS NULL")

# One monotonic publication slot belongs to exactly one artifact. A newer
# artifact must request the next revision; an intentional rollback does too.
if spark.catalog.tableExists(LEDGER):
    revision_conflict = spark.table(LEDGER).where(
        (F.col("ready_manifest_key") == ready_manifest_key)
        & (F.col("publication_revision") == publication_revision)
        & (F.col("transform_version") != transform_version))
    if revision_conflict.limit(1).count():
        raise ValueError("Publication revision is already assigned to another artifact")


def verify_persisted_run():
    checks = {
        "bronze_objects": (BRONZE, F.col("run_id") == run_id, len(output["bronze"])),
        "movie_observations": (OBS, (F.col("source_run_id") == run_id)
                               & (F.col("transform_version") == transform_version), len(movie_rows)),
        "boxoffice_observations": (BOX_OBS, (F.col("source_run_id") == run_id)
                                   & (F.col("transform_version") == transform_version), len(box_rows)),
    }
    for label, (table, condition, expected) in checks.items():
        if not spark.catalog.tableExists(table):
            raise ValueError(f"Expected persisted table is missing: {label}")
        actual = spark.table(table).where(condition).count()
        if actual != expected:
            raise ValueError(f"Persisted {label} count mismatch: expected={expected}, actual={actual}")


if spark.catalog.tableExists(LEDGER):
    replay = spark.table(LEDGER).where(
        (F.col("ready_manifest_key") == ready_manifest_key)
        & (F.col("transform_version") == transform_version)
        & (F.col("publication_revision") == publication_revision)
        & (F.col("status") == "SUCCESS"))
    if replay.count():
        previous = replay.orderBy(F.col("completed_at").desc()).first().asDict()
        if previous["movie_count"] != len(movie_rows) or previous["bronze_object_count"] != len(output["bronze"]):
            raise ValueError("Successful ledger counts conflict with replay input")
        verify_persisted_run()
        dbutils.notebook.exit(json.dumps({"status": "ALREADY_PROCESSED", "run_id": run_id}))


def ensure_table(table, frame):
    if not spark.catalog.tableExists(table):
        frame.limit(0).write.format("delta").saveAsTable(table)


def reject_conflict(table, incoming, condition, hash_condition, label):
    if not spark.catalog.tableExists(table):
        return
    target = spark.table(table).alias("target")
    if target.join(incoming.alias("source"), F.expr(condition)).where(F.expr(hash_condition)).limit(1).count():
        raise ValueError(f"Immutable {label} conflict")


ledger_schema = T.StructType([
    string_field("ready_manifest_key"), string_field("source_run_id"),
    string_field("transform_version"), long_field("publication_revision"),
    long_field("processing_attempt"),
    string_field("status"), string_field("error_class"),
    long_field("bronze_object_count"), long_field("movie_count"), long_field("boxoffice_row_count"),
])


def ledger_frame(status, error_class=None):
    return spark.createDataFrame([{
        "ready_manifest_key": ready_manifest_key, "source_run_id": run_id,
        "transform_version": transform_version, "processing_attempt": processing_attempt,
        "publication_revision": publication_revision,
        "status": status, "error_class": error_class,
        "bronze_object_count": len(output["bronze"]), "movie_count": len(movie_rows),
        "boxoffice_row_count": len(box_rows),
    }], ledger_schema).withColumn("updated_at", F.current_timestamp()) \
      .withColumn("completed_at", F.when(F.lit(status) == "SUCCESS", F.current_timestamp()))


def upsert_ledger(status, error_class=None):
    frame = ledger_frame(status, error_class)
    ensure_table(LEDGER, frame)
    DeltaTable.forName(spark, LEDGER).alias("target").merge(
        frame.alias("source"),
        "target.ready_manifest_key=source.ready_manifest_key AND target.transform_version=source.transform_version AND target.publication_revision=source.publication_revision AND target.processing_attempt=source.processing_attempt"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def publish_success_views():
    # Replace the integration-run mutable current tables with backups once.
    # Published views never expose observations until their ledger is SUCCESS.
    for current, backup in (
        (CURRENT, "workspace.silver.movies_current_legacy_20260909"),
        (BOX_CURRENT, "workspace.silver.boxoffice_current_legacy_20260909"),
    ):
        if spark.catalog.tableExists(current) and spark.catalog.getTable(current).tableType != "VIEW":
            if not spark.catalog.tableExists(backup):
                spark.sql(f"ALTER TABLE {current} RENAME TO {backup.split('.')[-1]}")
            else:
                raise ValueError(f"Cannot migrate published view because both current and backup exist: {current}")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {CURRENT} AS
        SELECT * EXCEPT (_publication_rank) FROM (
          SELECT o.*, ROW_NUMBER() OVER (
            PARTITION BY o.canonical_movie_key
            ORDER BY {transform.PUBLICATION_ORDER_SQL}
          ) AS _publication_rank
          FROM {OBS} o
          JOIN {LEDGER} l
            ON l.ready_manifest_key = o.ready_manifest_key
           AND l.transform_version = o.transform_version
           AND l.status = 'SUCCESS'
        ) WHERE _publication_rank = 1
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {BOX_CURRENT} AS
        SELECT * EXCEPT (_publication_rank) FROM (
          SELECT o.*, ROW_NUMBER() OVER (
            PARTITION BY o.target_date, o.kofic_movie_cd
            ORDER BY {transform.PUBLICATION_ORDER_SQL}
          ) AS _publication_rank
          FROM {BOX_OBS} o
          JOIN {LEDGER} l
            ON l.ready_manifest_key = o.ready_manifest_key
           AND l.transform_version = o.transform_version
           AND l.status = 'SUCCESS'
        ) WHERE _publication_rank = 1
    """)


upsert_ledger("RUNNING")
try:
    reject_conflict(BRONZE, bronze_df, "target.object_key=source.object_key",
                    "target.payload_sha256<>source.payload_sha256", "Bronze object")
    ensure_table(BRONZE, bronze_df)
    DeltaTable.forName(spark, BRONZE).alias("target").merge(
        bronze_df.alias("source"), "target.object_key=source.object_key"
    ).whenNotMatchedInsertAll().execute()

    reject_conflict(OBS, movie_df,
                    "target.source_run_id=source.source_run_id AND target.transform_version=source.transform_version AND target.canonical_movie_key=source.canonical_movie_key",
                    "target.content_sha256<>source.content_sha256 OR target.policy_sha256<>source.policy_sha256 OR target.matching_sha256<>source.matching_sha256",
                    "movie observation")
    ensure_table(OBS, movie_df)
    DeltaTable.forName(spark, OBS).alias("target").merge(
        movie_df.alias("source"),
        "target.source_run_id=source.source_run_id AND target.transform_version=source.transform_version AND target.canonical_movie_key=source.canonical_movie_key"
    ).whenNotMatchedInsertAll().execute()

    reject_conflict(BOX_OBS, box_df,
                    "target.source_run_id=source.source_run_id AND target.transform_version=source.transform_version AND target.target_date=source.target_date AND target.kofic_movie_cd=source.kofic_movie_cd",
                    "target.observation_sha256<>source.observation_sha256",
                    "boxoffice observation")
    ensure_table(BOX_OBS, box_df)
    DeltaTable.forName(spark, BOX_OBS).alias("target").merge(
        box_df.alias("source"),
        "target.source_run_id=source.source_run_id AND target.transform_version=source.transform_version AND target.target_date=source.target_date AND target.kofic_movie_cd=source.kofic_movie_cd"
    ).whenNotMatchedInsertAll().execute()
    publish_success_views()
    verify_persisted_run()
    upsert_ledger("SUCCESS")
except Exception as exc:
    upsert_ledger("FAILED", type(exc).__name__)
    raise

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "run_id": run_id,
                                  "bronze_object_count": len(output["bronze"]),
                                  "movie_count": len(movie_rows),
                                  "boxoffice_row_count": len(box_rows)}))
