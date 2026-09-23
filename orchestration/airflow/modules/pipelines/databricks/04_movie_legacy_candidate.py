# Databricks notebook source
# MAGIC %md
# MAGIC # Pop Talk Legacy 영화 candidate Bronze/Silver
# MAGIC
# MAGIC Airflow가 검증해 전달한 불변 bundle을 같은 순수 함수로 다시 계산한다.
# MAGIC 운영 Bronze/Silver current는 변경하지 않고 candidate 관측과 실행 원장만 기록한다.

# COMMAND ----------

import hashlib
import io
import json
import types
import zipfile

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

dbutils.widgets.text("landing_dir", "", "Immutable candidate landing directory")
dbutils.widgets.text("manifest_key", "", "Exact Legacy SUCCESS manifest key")
dbutils.widgets.text("artifact_version", "", "Processing artifact SHA-256")
dbutils.widgets.text("source_sha256", "", "Validated Legacy source SHA-256")
dbutils.widgets.text("archive_sha256", "", "Immutable staged archive SHA-256")
dbutils.widgets.text("publication_revision", "1", "Candidate publication revision")
dbutils.widgets.text("processing_attempt", "1", "Explicit remote processing attempt")

landing_dir = dbutils.widgets.get("landing_dir").rstrip("/")
manifest_key = dbutils.widgets.get("manifest_key").strip()
artifact_version = dbutils.widgets.get("artifact_version").strip()
source_sha256 = dbutils.widgets.get("source_sha256").strip()
archive_sha256 = dbutils.widgets.get("archive_sha256").strip()
publication_revision = int(dbutils.widgets.get("publication_revision"))
processing_attempt = int(dbutils.widgets.get("processing_attempt"))

LANDING_PREFIX = "/Volumes/workspace/bronze/landing/movie_legacy_candidate/"
if len(artifact_version) != 64 or any(c not in "0123456789abcdef" for c in artifact_version):
    raise ValueError("Invalid Legacy candidate artifact version")
if len(source_sha256) != 64 or any(c not in "0123456789abcdef" for c in source_sha256):
    raise ValueError("Invalid Legacy candidate source SHA-256")
if len(archive_sha256) != 64 or any(c not in "0123456789abcdef" for c in archive_sha256):
    raise ValueError("Invalid Legacy candidate archive SHA-256")
expected_landing = f"{LANDING_PREFIX}{source_sha256}/{artifact_version}"
if landing_dir != expected_landing:
    raise ValueError("Legacy candidate landing identity mismatch")
if manifest_key != (
    f"manifests/legacy_snapshot/v1/sha256={source_sha256}/SUCCESS.json"
):
    raise ValueError("Legacy candidate manifest identity mismatch")
if publication_revision < 1 or processing_attempt < 1:
    raise ValueError("Candidate revision and attempt must be positive")

with open(f"{landing_dir}/bundle.zip", "rb") as archive_file:
    archive_body = archive_file.read()
if hashlib.sha256(archive_body).hexdigest() != archive_sha256:
    raise ValueError("Legacy candidate archive digest mismatch")
bundle = zipfile.ZipFile(io.BytesIO(archive_body))
required_documents = {
    "SUCCESS.json", "source/movies_final.json", "movie_bronze_silver.py",
    "silver_movies.jsonl", "silver_boxoffice.jsonl",
}
names = bundle.namelist()
if len(names) != len(set(names)) or set(names) != required_documents | {"bundle_index.json"}:
    raise ValueError("Candidate bundle file set is invalid")


def read_bytes(relative):
    return bundle.read(relative)


index = json.loads(read_bytes("bundle_index.json"))
if (
    index.get("bundle_version") != 2
    or index.get("exchange_contract_version") != 2
    or index.get("manifest_key") != manifest_key
    or index.get("source_sha256") != source_sha256
    or index.get("source_run_id") != f"legacy:{source_sha256}"
    or index.get("source_key")
    != f"raw/legacy_snapshot/v1/sha256={source_sha256}/movies_final.json"
    or index.get("artifact_version") != artifact_version
    or set(index.get("documents") or {}) != required_documents
):
    raise ValueError("Candidate bundle identity is invalid")
for relative, metadata in index["documents"].items():
    body = read_bytes(relative)
    if hashlib.sha256(body).hexdigest() != metadata["sha256"] or len(body) != metadata["bytes"]:
        raise ValueError(f"Candidate bundle checksum mismatch: {relative}")

transform = types.ModuleType("pop_talk_legacy_candidate_transform")
exec(
    compile(read_bytes("movie_bronze_silver.py"), "movie_bronze_silver.py", "exec"),
    transform.__dict__,
)
manifest = json.loads(read_bytes("SUCCESS.json"))
source_body = read_bytes("source/movies_final.json")
output = transform.transform_legacy_snapshot(manifest, source_body)
exchange = transform.legacy_exchange_observations(
    output,
    manifest_key=manifest_key,
    transform_version=artifact_version,
)
computed_movies = b"".join(transform.packed(row) + b"\n" for row in exchange["silver_movies"])
if computed_movies != read_bytes("silver_movies.jsonl"):
    raise ValueError("Candidate bundled Silver differs from remote recomputation")
if exchange["silver_boxoffice"] != [] or read_bytes("silver_boxoffice.jsonl") != b"":
    raise ValueError("Legacy candidate boxoffice must be an empty file")
if (
    len(exchange["silver_movies"]) != index["expected"]["silver_movies"]
    or output["quality"]["eligible_count"] != index["expected"]["eligible_movies"]
    or output["quality"]["excluded_count"] != index["expected"]["excluded_movies"]
):
    raise ValueError("Candidate recomputed quality counts differ from bundle")

source_run_id = index["source_run_id"]
source_sha256 = index["source_sha256"]

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

BRONZE = "workspace.bronze.movie_legacy_candidate_raw"
OBSERVATIONS = "workspace.silver.movie_legacy_candidate_observations"
LEDGER = "workspace.silver.movie_legacy_candidate_runs"


def string_field(name):
    return T.StructField(name, T.StringType(), True)


def bool_field(name):
    return T.StructField(name, T.BooleanType(), True)


def long_field(name):
    return T.StructField(name, T.LongType(), True)


array_names = (
    "countries", "genres", "directors", "director_names_en", "actors",
    "actor_roles", "production_companies", "posters", "stills", "keywords",
    "policy_exclusion_reasons",
)
string_names = (
    "canonical_movie_key", "kofic_movie_cd", "kmdb_id", "kmdb_mapping_status",
    "provisional_kmdb_id", "title_ko", "title_en", "title_original", "release_date",
    "movie_type", "production_status", "viewing_grade", "poster_url", "plot",
    "kmdb_rating", "vod_url", "policy_version", "ingestion_source", "source_run_id",
    "source_object_key", "source_service_status", "source_approval_status",
    "content_sha256", "policy_sha256", "embedding_input_sha256", "matching_sha256",
    "source_time_basis", "ready_manifest_key", "transform_version",
)
movie_schema = T.StructType(
    [string_field(name) for name in string_names]
    + [
        long_field(name) for name in (
            "service_movie_id", "production_year", "runtime_minutes", "kmdb_candidate_count",
        )
    ]
    + [
        bool_field(name) for name in (
            "kmdb_matched", "kmdb_candidates_truncated", "kmdb_candidates_ambiguous",
            "policy_eligible", "source_observed_at_known",
        )
    ]
    + [T.StructField(name, T.ArrayType(T.StringType()), True) for name in array_names]
    + [T.StructField("source_observed_at", T.TimestampType(), True)]
)

movie_records = []
for row in exchange["silver_movies"]:
    movie_records.append({
        **row,
        "record_sha256": hashlib.sha256(transform.packed(row)).hexdigest(),
    })
movie_df = spark.createDataFrame(
    movie_records,
    movie_schema.add(string_field("record_sha256")),
).withColumn("processed_at", F.current_timestamp())

bronze_schema = T.StructType([
    string_field("source_run_id"), string_field("manifest_key"),
    string_field("source_object_key"), string_field("source_sha256"),
    string_field("payload_json"),
])
bronze_df = spark.createDataFrame([{
    "source_run_id": source_run_id,
    "manifest_key": manifest_key,
    "source_object_key": index["source_key"],
    "source_sha256": source_sha256,
    "payload_json": source_body.decode("utf-8"),
}], bronze_schema).withColumn("ingested_at", F.current_timestamp())

ledger_schema = T.StructType([
    string_field("manifest_key"), string_field("source_run_id"),
    string_field("source_sha256"), string_field("artifact_version"),
    long_field("publication_revision"), long_field("processing_attempt"),
    string_field("status"), string_field("error_class"),
    long_field("bronze_object_count"), long_field("movie_count"),
    long_field("eligible_count"), long_field("excluded_count"),
])


def ensure_table(table, frame):
    if not spark.catalog.tableExists(table):
        frame.limit(0).write.format("delta").saveAsTable(table)


def ledger_frame(status, error_class=None):
    return spark.createDataFrame([{
        "manifest_key": manifest_key,
        "source_run_id": source_run_id,
        "source_sha256": source_sha256,
        "artifact_version": artifact_version,
        "publication_revision": publication_revision,
        "processing_attempt": processing_attempt,
        "status": status,
        "error_class": error_class,
        "bronze_object_count": 1,
        "movie_count": len(movie_records),
        "eligible_count": output["quality"]["eligible_count"],
        "excluded_count": output["quality"]["excluded_count"],
    }], ledger_schema).withColumn("updated_at", F.current_timestamp()).withColumn(
        "completed_at",
        F.when(F.lit(status) == "SUCCESS", F.current_timestamp()),
    )


def upsert_ledger(status, error_class=None):
    frame = ledger_frame(status, error_class)
    ensure_table(LEDGER, frame)
    DeltaTable.forName(spark, LEDGER).alias("target").merge(
        frame.alias("source"),
        "target.manifest_key=source.manifest_key AND "
        "target.artifact_version=source.artifact_version AND "
        "target.publication_revision=source.publication_revision AND "
        "target.processing_attempt=source.processing_attempt",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def verify_persisted():
    if not spark.catalog.tableExists(BRONZE) or not spark.catalog.tableExists(OBSERVATIONS):
        raise ValueError("Candidate persisted table is missing")
    bronze = spark.table(BRONZE).where(F.col("source_object_key") == index["source_key"])
    if bronze.count() != 1 or bronze.first()["source_sha256"] != source_sha256:
        raise ValueError("Candidate Bronze object count/hash mismatch")
    persisted = spark.table(OBSERVATIONS).where(
        (F.col("source_run_id") == source_run_id)
        & (F.col("transform_version") == artifact_version)
    )
    if persisted.count() != len(movie_records):
        raise ValueError("Candidate Silver persisted count mismatch")
    incoming_hashes = movie_df.select("canonical_movie_key", "record_sha256")
    mismatch = incoming_hashes.alias("source").join(
        persisted.select("canonical_movie_key", "record_sha256").alias("target"),
        "canonical_movie_key",
        "left",
    ).where(
        F.col("target.record_sha256").isNull()
        | (F.col("target.record_sha256") != F.col("source.record_sha256"))
    )
    if mismatch.limit(1).count():
        raise ValueError("Candidate Silver key/hash mismatch")


ensure_table(BRONZE, bronze_df)
ensure_table(OBSERVATIONS, movie_df)
ensure_table(LEDGER, ledger_frame("RUNNING"))

successful = spark.table(LEDGER).where(
    (F.col("manifest_key") == manifest_key)
    & (F.col("artifact_version") == artifact_version)
    & (F.col("publication_revision") == publication_revision)
    & (F.col("status") == "SUCCESS")
)
if successful.limit(1).count():
    verify_persisted()
    dbutils.notebook.exit(json.dumps({
        "status": "ALREADY_PROCESSED", "source_run_id": source_run_id,
        "movie_count": len(movie_records),
    }))

# COMMAND ----------

try:
    upsert_ledger("RUNNING")

    bronze_conflict = spark.table(BRONZE).where(
        (F.col("source_object_key") == index["source_key"])
        & (
            (F.col("source_sha256") != source_sha256)
            | (F.col("payload_json") != source_body.decode("utf-8"))
        )
    )
    if bronze_conflict.limit(1).count():
        raise ValueError("Immutable candidate Bronze conflict")
    DeltaTable.forName(spark, BRONZE).alias("target").merge(
        bronze_df.alias("source"),
        "target.source_object_key=source.source_object_key",
    ).whenNotMatchedInsertAll().execute()

    observation_conflict = spark.table(OBSERVATIONS).alias("target").join(
        movie_df.alias("source"),
        (F.col("target.source_run_id") == F.col("source.source_run_id"))
        & (F.col("target.transform_version") == F.col("source.transform_version"))
        & (F.col("target.canonical_movie_key") == F.col("source.canonical_movie_key")),
    ).where(F.col("target.record_sha256") != F.col("source.record_sha256"))
    if observation_conflict.limit(1).count():
        raise ValueError("Immutable candidate Silver conflict")
    DeltaTable.forName(spark, OBSERVATIONS).alias("target").merge(
        movie_df.alias("source"),
        "target.source_run_id=source.source_run_id AND "
        "target.transform_version=source.transform_version AND "
        "target.canonical_movie_key=source.canonical_movie_key",
    ).whenNotMatchedInsertAll().execute()

    verify_persisted()
    upsert_ledger("SUCCESS")
except Exception as error:
    try:
        upsert_ledger("FAILED", type(error).__name__)
    finally:
        raise

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "source_run_id": source_run_id,
    "source_sha256": source_sha256,
    "artifact_version": artifact_version,
    "movie_count": len(movie_records),
    "eligible_count": output["quality"]["eligible_count"],
    "excluded_count": output["quality"]["excluded_count"],
}))
