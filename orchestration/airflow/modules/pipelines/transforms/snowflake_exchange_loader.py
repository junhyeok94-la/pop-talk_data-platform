"""Validate an S3 Silver exchange publication and load Snowflake observations."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


class SnowflakeLoadError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExchangeInput:
    ready_key: str
    source_run_id: str
    artifact_version: str
    publication_revision: int
    bundle_sha256: str
    movies: tuple[dict, ...]
    boxoffice: tuple[dict, ...]


def _read(client: Any, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def read_exchange(client: Any, *, bucket: str, ready_key: str) -> ExchangeInput:
    ready_body = _read(client, bucket, ready_key)
    try:
        ready = json.loads(ready_body)
    except (UnicodeError, ValueError):
        raise SnowflakeLoadError("EXCHANGE_READY is not valid UTF-8 JSON") from None
    if ready.get("schema_version") != 1 or ready.get("exchange_contract_version") != 1:
        raise SnowflakeLoadError("Unsupported exchange contract")
    required = {"movies.jsonl", "boxoffice.jsonl"}
    files = ready.get("files")
    if not isinstance(files, dict) or set(files) != required:
        raise SnowflakeLoadError("EXCHANGE_READY file set is incomplete")

    parsed: dict[str, tuple[dict, ...]] = {}
    for name in sorted(required):
        meta = files[name]
        body = _read(client, bucket, meta["key"])
        if hashlib.sha256(body).hexdigest() != meta["sha256"]:
            raise SnowflakeLoadError(f"Exchange checksum mismatch: {name}")
        try:
            rows = tuple(json.loads(line) for line in body.splitlines() if line.strip())
        except (UnicodeError, ValueError):
            raise SnowflakeLoadError(f"Exchange file is not valid JSONL: {name}") from None
        if any(not isinstance(row, dict) for row in rows) or len(rows) != meta["row_count"]:
            raise SnowflakeLoadError(f"Exchange row contract mismatch: {name}")
        parsed[name] = rows

    run_id = str(ready["source_run_id"])
    artifact = str(ready["artifact_version"])
    revision = int(ready["publication_revision"])
    expected_ready_key = (
        "exchange/movie_silver/v1/"
        f"source_run_id={run_id}/artifact_version={artifact}/"
        f"publication_revision={revision}/EXCHANGE_READY.json"
    )
    if ready_key != expected_ready_key:
        raise SnowflakeLoadError("EXCHANGE_READY path identity mismatch")
    for name, rows in parsed.items():
        business = set()
        for row in rows:
            if row.get("source_run_id") != run_id or row.get("transform_version") != artifact:
                raise SnowflakeLoadError(f"Exchange row identity mismatch: {name}")
            if not row.get("source_observed_at") or row.get("ready_manifest_key") != ready["source_ready_key"]:
                raise SnowflakeLoadError(f"Exchange observation metadata missing: {name}")
            key = (row.get("canonical_movie_key"),) if name == "movies.jsonl" else (
                row.get("target_date"), row.get("kofic_movie_cd"))
            if None in key or key in business:
                raise SnowflakeLoadError(f"Exchange business key invalid or duplicated: {name}")
            business.add(key)
    return ExchangeInput(ready_key, run_id, artifact, revision,
                         str(ready["source_bundle_sha256"]),
                         parsed["movies.jsonl"], parsed["boxoffice.jsonl"])


DDL = """
CREATE TABLE IF NOT EXISTS POP_TALK_DW_DEV.STAGING.SILVER_EXCHANGE_LOADS (
  EXCHANGE_READY_KEY STRING NOT NULL, SOURCE_RUN_ID STRING NOT NULL,
  ARTIFACT_VERSION STRING NOT NULL, PUBLICATION_REVISION NUMBER NOT NULL,
  SOURCE_BUNDLE_SHA256 STRING NOT NULL, STATUS STRING NOT NULL,
  MOVIE_COUNT NUMBER, BOXOFFICE_COUNT NUMBER,
  UPDATED_AT TIMESTAMP_TZ NOT NULL, COMPLETED_AT TIMESTAMP_TZ,
  PRIMARY KEY (EXCHANGE_READY_KEY)
);
CREATE TABLE IF NOT EXISTS POP_TALK_DW_DEV.STAGING.MOVIE_OBSERVATIONS_RAW (
  SOURCE_RUN_ID STRING NOT NULL, ARTIFACT_VERSION STRING NOT NULL,
  PUBLICATION_REVISION NUMBER NOT NULL, CANONICAL_MOVIE_KEY STRING NOT NULL,
  SOURCE_OBSERVED_AT TIMESTAMP_TZ NOT NULL, RECORD_SHA256 STRING NOT NULL,
  PAYLOAD VARIANT NOT NULL, EXCHANGE_READY_KEY STRING NOT NULL,
  LOADED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
CREATE TABLE IF NOT EXISTS POP_TALK_DW_DEV.STAGING.BOXOFFICE_OBSERVATIONS_RAW (
  SOURCE_RUN_ID STRING NOT NULL, ARTIFACT_VERSION STRING NOT NULL,
  PUBLICATION_REVISION NUMBER NOT NULL, TARGET_DATE DATE NOT NULL,
  KOFIC_MOVIE_CD STRING NOT NULL, SOURCE_OBSERVED_AT TIMESTAMP_TZ NOT NULL,
  RECORD_SHA256 STRING NOT NULL, PAYLOAD VARIANT NOT NULL,
  EXCHANGE_READY_KEY STRING NOT NULL, LOADED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
"""


def _payload(row: dict) -> tuple[str, str]:
    text = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text, hashlib.sha256(text.encode()).hexdigest()


def load_exchange(connection: Any, exchange: ExchangeInput, *,
                  fail_after_movie_merge: bool = False) -> tuple[int, int]:
    """Load one publication transactionally; DDL is idempotent setup."""
    cursor = connection.cursor()
    try:
        for statement in (part.strip() for part in DDL.split(";") if part.strip()):
            cursor.execute(statement)
        # Snowflake DDL implicitly commits. Prepare temporary input completely
        # before opening the publication transaction.
        cursor.execute("CREATE OR REPLACE TEMP TABLE INCOMING_MOVIES (PAYLOAD_TEXT STRING, RECORD_SHA256 STRING)")
        cursor.execute("CREATE OR REPLACE TEMP TABLE INCOMING_BOXOFFICE (PAYLOAD_TEXT STRING, RECORD_SHA256 STRING)")
        if exchange.movies:
            cursor.executemany("INSERT INTO INCOMING_MOVIES VALUES (%s,%s)", [_payload(r) for r in exchange.movies])
        if exchange.boxoffice:
            cursor.executemany("INSERT INTO INCOMING_BOXOFFICE VALUES (%s,%s)", [_payload(r) for r in exchange.boxoffice])

        identity = (exchange.source_run_id, exchange.artifact_version,
                    exchange.publication_revision, exchange.bundle_sha256)

        def verify_persisted() -> None:
            specs = (
                ("MOVIE_OBSERVATIONS_RAW", "INCOMING_MOVIES", len(exchange.movies),
                 "t.CANONICAL_MOVIE_KEY=s.p:canonical_movie_key::STRING"),
                ("BOXOFFICE_OBSERVATIONS_RAW", "INCOMING_BOXOFFICE", len(exchange.boxoffice),
                 "t.TARGET_DATE=s.p:target_date::DATE AND t.KOFIC_MOVIE_CD=s.p:kofic_movie_cd::STRING"),
            )
            for table, incoming, expected, key_join in specs:
                cursor.execute(f"""SELECT COUNT(*) FROM POP_TALK_DW_DEV.STAGING.{table}
                  WHERE SOURCE_RUN_ID=%s AND ARTIFACT_VERSION=%s""", identity[:2])
                if cursor.fetchone()[0] != expected:
                    raise SnowflakeLoadError(f"Persisted Snowflake count mismatch: {table}")
                cursor.execute(f"""SELECT COUNT(*) FROM
                  (SELECT PARSE_JSON(PAYLOAD_TEXT) p, RECORD_SHA256 FROM {incoming}) s
                  LEFT JOIN POP_TALK_DW_DEV.STAGING.{table} t
                    ON t.SOURCE_RUN_ID=%s AND t.ARTIFACT_VERSION=%s AND {key_join}
                  WHERE t.RECORD_SHA256 IS NULL OR t.RECORD_SHA256<>s.RECORD_SHA256""", identity[:2])
                if cursor.fetchone()[0]:
                    raise SnowflakeLoadError(f"Persisted Snowflake key/hash mismatch: {table}")

        cursor.execute("BEGIN")
        cursor.execute("""SELECT SOURCE_RUN_ID, ARTIFACT_VERSION, PUBLICATION_REVISION,
                          SOURCE_BUNDLE_SHA256, STATUS, MOVIE_COUNT, BOXOFFICE_COUNT
                          FROM POP_TALK_DW_DEV.STAGING.SILVER_EXCHANGE_LOADS
                          WHERE EXCHANGE_READY_KEY=%s""", (exchange.ready_key,))
        previous = cursor.fetchone()
        if previous and tuple(previous[:4]) != identity:
            raise SnowflakeLoadError("Snowflake load ledger identity conflict")
        if previous and previous[4] == "SUCCESS":
            if tuple(previous[5:7]) != (len(exchange.movies), len(exchange.boxoffice)):
                raise SnowflakeLoadError("Snowflake successful replay count conflict")
            verify_persisted()
            connection.commit()
            return len(exchange.movies), len(exchange.boxoffice)

        cursor.execute("""MERGE INTO POP_TALK_DW_DEV.STAGING.SILVER_EXCHANGE_LOADS t
          USING (SELECT %s k,%s r,%s a,%s p,%s b) s ON t.EXCHANGE_READY_KEY=s.k
          WHEN MATCHED THEN UPDATE SET STATUS='RUNNING',UPDATED_AT=CURRENT_TIMESTAMP()
          WHEN NOT MATCHED THEN INSERT (EXCHANGE_READY_KEY,SOURCE_RUN_ID,ARTIFACT_VERSION,
          PUBLICATION_REVISION,SOURCE_BUNDLE_SHA256,STATUS,UPDATED_AT)
          VALUES(s.k,s.r,s.a,s.p,s.b,'RUNNING',CURRENT_TIMESTAMP())""",
                       (exchange.ready_key, *identity))
        for target, incoming, key_expr, fields in (
            ("MOVIE_OBSERVATIONS_RAW", "INCOMING_MOVIES", "s.p:canonical_movie_key::STRING",
             "CANONICAL_MOVIE_KEY"),
            ("BOXOFFICE_OBSERVATIONS_RAW", "INCOMING_BOXOFFICE",
             "p:target_date::DATE || '|' || p:kofic_movie_cd::STRING", "TARGET_DATE,KOFIC_MOVIE_CD"),
        ):
            cursor.execute(f"""SELECT COUNT(*) FROM POP_TALK_DW_DEV.STAGING.{target} t
              JOIN (SELECT PARSE_JSON(PAYLOAD_TEXT) p, RECORD_SHA256 FROM {incoming}) s
              ON t.SOURCE_RUN_ID=%s AND t.ARTIFACT_VERSION=%s AND
                 {('t.CANONICAL_MOVIE_KEY=' + key_expr) if target.startswith('MOVIE') else "t.TARGET_DATE=s.p:target_date::DATE AND t.KOFIC_MOVIE_CD=s.p:kofic_movie_cd::STRING"}
              WHERE t.RECORD_SHA256<>s.RECORD_SHA256""", identity[:2])
            if cursor.fetchone()[0]:
                raise SnowflakeLoadError(f"Immutable Snowflake observation conflict: {target}")
            if target.startswith("MOVIE"):
                select_fields = ("p:canonical_movie_key::STRING AS CANONICAL_MOVIE_KEY,"
                                 "p:source_observed_at::TIMESTAMP_TZ AS SOURCE_OBSERVED_AT")
                join = "t.CANONICAL_MOVIE_KEY=s.CANONICAL_MOVIE_KEY"
                aliases = "CANONICAL_MOVIE_KEY,SOURCE_OBSERVED_AT"
            else:
                select_fields = ("p:target_date::DATE AS TARGET_DATE,"
                                 "p:kofic_movie_cd::STRING AS KOFIC_MOVIE_CD,"
                                 "p:source_observed_at::TIMESTAMP_TZ AS SOURCE_OBSERVED_AT")
                join = "t.TARGET_DATE=s.TARGET_DATE AND t.KOFIC_MOVIE_CD=s.KOFIC_MOVIE_CD"
                aliases = "TARGET_DATE,KOFIC_MOVIE_CD,SOURCE_OBSERVED_AT"
            cursor.execute(f"""MERGE INTO POP_TALK_DW_DEV.STAGING.{target} t USING (
              SELECT %s SOURCE_RUN_ID,%s ARTIFACT_VERSION,%s PUBLICATION_REVISION,
                     {select_fields},RECORD_SHA256,PARSE_JSON(PAYLOAD_TEXT) PAYLOAD,%s EXCHANGE_READY_KEY
              FROM (SELECT PAYLOAD_TEXT, PARSE_JSON(PAYLOAD_TEXT) p, RECORD_SHA256 FROM {incoming})) s
              ON t.SOURCE_RUN_ID=s.SOURCE_RUN_ID AND t.ARTIFACT_VERSION=s.ARTIFACT_VERSION AND {join}
              WHEN NOT MATCHED THEN INSERT (SOURCE_RUN_ID,ARTIFACT_VERSION,PUBLICATION_REVISION,
                {aliases},RECORD_SHA256,PAYLOAD,EXCHANGE_READY_KEY)
              VALUES(s.SOURCE_RUN_ID,s.ARTIFACT_VERSION,s.PUBLICATION_REVISION,{','.join('s.'+x for x in aliases.split(','))},s.RECORD_SHA256,s.PAYLOAD,s.EXCHANGE_READY_KEY)""",
                           (*identity[:3], exchange.ready_key))
            if target.startswith("MOVIE") and fail_after_movie_merge:
                raise SnowflakeLoadError("Injected failure after movie merge")
        verify_persisted()
        cursor.execute("""UPDATE POP_TALK_DW_DEV.STAGING.SILVER_EXCHANGE_LOADS SET
          STATUS='SUCCESS',MOVIE_COUNT=%s,BOXOFFICE_COUNT=%s,
          UPDATED_AT=CURRENT_TIMESTAMP(),COMPLETED_AT=CURRENT_TIMESTAMP()
          WHERE EXCHANGE_READY_KEY=%s""",
                       (len(exchange.movies), len(exchange.boxoffice), exchange.ready_key))
        connection.commit()
        return len(exchange.movies), len(exchange.boxoffice)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
