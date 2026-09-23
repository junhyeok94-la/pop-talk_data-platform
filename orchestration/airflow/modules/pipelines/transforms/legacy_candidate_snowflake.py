"""S3 Legacy candidate Exchange v2를 Snowflake CANDIDATE에 적재한다."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from pipelines.transforms.legacy_candidate_exchange import EMPTY_SHA256
from pipelines.transforms.movie_bronze_silver import packed


class LegacyCandidateLoadError(RuntimeError):
    """candidate 입력 또는 트랜잭션 계약 위반."""


READY_PATTERN = re.compile(
    r"^exchange/candidate/movie_legacy/v2/"
    r"source_sha256=([0-9a-f]{64})/artifact_version=([0-9a-f]{64})/"
    r"publication_revision=([1-9][0-9]*)/EXCHANGE_READY[.]json$"
)


@dataclass(frozen=True)
class LegacyCandidateInput:
    ready_key: str
    source_run_id: str
    source_sha256: str
    artifact_version: str
    publication_revision: int
    bundle_sha256: str
    manifest_key: str
    movies: tuple[dict[str, Any], ...]


def _read(client: Any, bucket: str, key: str) -> bytes:
    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        raise LegacyCandidateLoadError("Candidate Exchange S3 read failed") from None


def read_legacy_candidate_exchange(
    client: Any, *, bucket: str, ready_key: str
) -> LegacyCandidateInput:
    """파일 GET 전에 READY와 모든 child key의 candidate allowlist를 검증한다."""
    path = READY_PATTERN.fullmatch(ready_key)
    if not path:
        raise LegacyCandidateLoadError("Candidate READY key is outside the allowlist")
    source_sha, artifact, revision_text = path.groups()
    revision = int(revision_text)
    prefix = ready_key.rsplit("/", 1)[0]
    try:
        ready = json.loads(_read(client, bucket, ready_key))
    except LegacyCandidateLoadError:
        raise
    except (UnicodeError, ValueError):
        raise LegacyCandidateLoadError("Candidate READY is not valid JSON") from None
    source_run_id = f"legacy:{source_sha}"
    if (
        not isinstance(ready, dict)
        or ready.get("schema_version") != 2
        or ready.get("exchange_contract_version") != 2
        or ready.get("source_sha256") != source_sha
        or ready.get("source_run_id") != source_run_id
        or ready.get("artifact_version") != artifact
        or ready.get("publication_revision") != revision
    ):
        raise LegacyCandidateLoadError("Candidate READY/path identity mismatch")
    bundle_sha = ready.get("source_bundle_sha256")
    if not isinstance(bundle_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", bundle_sha):
        raise LegacyCandidateLoadError("Candidate source bundle digest is invalid")
    manifest_key = ready.get("source_manifest_key")
    source_key = ready.get("source_object_key")
    if manifest_key != (
        f"manifests/legacy_snapshot/v1/sha256={source_sha}/SUCCESS.json"
    ) or source_key != (
        f"raw/legacy_snapshot/v1/sha256={source_sha}/movies_final.json"
    ):
        raise LegacyCandidateLoadError("Candidate source lineage path mismatch")
    files = ready.get("files")
    required = {"movies.jsonl", "boxoffice.jsonl"}
    if not isinstance(files, dict) or set(files) != required:
        raise LegacyCandidateLoadError("Candidate READY file set is invalid")
    for name in sorted(required):
        metadata = files[name]
        if not isinstance(metadata, dict) or metadata.get("key") != f"{prefix}/{name}":
            raise LegacyCandidateLoadError("Candidate child key is outside its publication prefix")
        if (
            not isinstance(metadata.get("bytes"), int)
            or metadata["bytes"] < 0
            or not isinstance(metadata.get("row_count"), int)
            or metadata["row_count"] < 0
            or not isinstance(metadata.get("sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", metadata["sha256"])
        ):
            raise LegacyCandidateLoadError("Candidate file metadata is invalid")
    box_meta = files["boxoffice.jsonl"]
    if (
        box_meta["bytes"] != 0
        or box_meta["row_count"] != 0
        or box_meta["sha256"] != EMPTY_SHA256
    ):
        raise LegacyCandidateLoadError("Legacy boxoffice metadata must describe empty bytes")

    bodies = {name: _read(client, bucket, files[name]["key"]) for name in sorted(required)}
    for name, body in bodies.items():
        metadata = files[name]
        if len(body) != metadata["bytes"] or hashlib.sha256(body).hexdigest() != metadata["sha256"]:
            raise LegacyCandidateLoadError(f"Candidate file checksum mismatch: {name}")
    if bodies["boxoffice.jsonl"] != b"":
        raise LegacyCandidateLoadError("Legacy boxoffice file must be empty bytes")

    movie_body = bodies["movies.jsonl"]
    if movie_body and not movie_body.endswith(b"\n"):
        raise LegacyCandidateLoadError("Candidate movie JSONL must end with LF")
    try:
        movies = tuple(json.loads(line) for line in movie_body.splitlines())
    except (UnicodeError, ValueError):
        raise LegacyCandidateLoadError("Candidate movie file is not valid JSONL") from None
    if any(not isinstance(row, dict) for row in movies):
        raise LegacyCandidateLoadError("Candidate movie row must be an object")
    if b"".join(packed(row) + b"\n" for row in movies) != movie_body:
        raise LegacyCandidateLoadError("Candidate movie JSONL is not canonical")
    if len(movies) != files["movies.jsonl"]["row_count"]:
        raise LegacyCandidateLoadError("Candidate movie row count mismatch")
    business_keys: set[str] = set()
    for row in movies:
        key = row.get("canonical_movie_key")
        if not isinstance(key, str) or not key or key in business_keys:
            raise LegacyCandidateLoadError("Candidate movie business key is invalid or duplicated")
        business_keys.add(key)
        if (
            row.get("source_run_id") != source_run_id
            or row.get("transform_version") != artifact
            or row.get("ready_manifest_key") != manifest_key
            or row.get("source_observed_at") is not None
            or row.get("source_observed_at_known") is not False
            or row.get("source_time_basis") != "LEGACY_UNKNOWN"
        ):
            raise LegacyCandidateLoadError("Legacy movie time or lineage contract mismatch")
    return LegacyCandidateInput(
        ready_key=ready_key,
        source_run_id=source_run_id,
        source_sha256=source_sha,
        artifact_version=artifact,
        publication_revision=revision,
        bundle_sha256=bundle_sha,
        manifest_key=manifest_key,
        movies=movies,
    )


DDL = """
CREATE SCHEMA IF NOT EXISTS POP_TALK_DW_DEV.CANDIDATE;
CREATE TABLE IF NOT EXISTS POP_TALK_DW_DEV.CANDIDATE.LEGACY_EXCHANGE_LOADS (
  EXCHANGE_READY_KEY STRING NOT NULL, SOURCE_RUN_ID STRING NOT NULL,
  SOURCE_SHA256 STRING NOT NULL, ARTIFACT_VERSION STRING NOT NULL,
  PUBLICATION_REVISION NUMBER NOT NULL, SOURCE_BUNDLE_SHA256 STRING NOT NULL,
  STATUS STRING NOT NULL, MOVIE_COUNT NUMBER, UPDATED_AT TIMESTAMP_TZ NOT NULL,
  COMPLETED_AT TIMESTAMP_TZ, PRIMARY KEY (EXCHANGE_READY_KEY)
);
CREATE TABLE IF NOT EXISTS POP_TALK_DW_DEV.CANDIDATE.MOVIE_OBSERVATIONS_RAW (
  SOURCE_RUN_ID STRING NOT NULL, SOURCE_SHA256 STRING NOT NULL,
  ARTIFACT_VERSION STRING NOT NULL, PUBLICATION_REVISION NUMBER NOT NULL,
  CANONICAL_MOVIE_KEY STRING NOT NULL, SOURCE_OBSERVED_AT TIMESTAMP_TZ,
  SOURCE_OBSERVED_AT_KNOWN BOOLEAN NOT NULL, SOURCE_TIME_BASIS STRING NOT NULL,
  RECORD_SHA256 STRING NOT NULL, PAYLOAD VARIANT NOT NULL,
  EXCHANGE_READY_KEY STRING NOT NULL,
  LOADED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
"""


def _payload(row: dict[str, Any]) -> tuple[str, str]:
    body = packed(row)
    return body.decode("utf-8"), hashlib.sha256(body).hexdigest()


def validate_legacy_candidate_input(exchange: LegacyCandidateInput) -> None:
    """reader 우회 호출에서도 DDL 전에 동일 lineage/time 계약을 강제한다."""
    path = READY_PATTERN.fullmatch(exchange.ready_key)
    if not path:
        raise LegacyCandidateLoadError("Candidate load READY key is outside the allowlist")
    source_sha, artifact, revision_text = path.groups()
    if (
        exchange.source_sha256 != source_sha
        or exchange.source_run_id != f"legacy:{source_sha}"
        or exchange.artifact_version != artifact
        or exchange.publication_revision != int(revision_text)
        or exchange.manifest_key
        != f"manifests/legacy_snapshot/v1/sha256={source_sha}/SUCCESS.json"
        or not re.fullmatch(r"[0-9a-f]{64}", exchange.bundle_sha256)
    ):
        raise LegacyCandidateLoadError("Candidate load input identity mismatch")
    keys: set[str] = set()
    for row in exchange.movies:
        key = row.get("canonical_movie_key")
        if not isinstance(key, str) or not key or key in keys:
            raise LegacyCandidateLoadError("Candidate load movie key is invalid or duplicated")
        keys.add(key)
        if (
            row.get("source_run_id") != exchange.source_run_id
            or row.get("transform_version") != exchange.artifact_version
            or row.get("ready_manifest_key") != exchange.manifest_key
            or row.get("source_observed_at") is not None
            or row.get("source_observed_at_known") is not False
            or row.get("source_time_basis") != "LEGACY_UNKNOWN"
        ):
            raise LegacyCandidateLoadError("Candidate load movie lineage/time contract mismatch")


def load_legacy_candidate(
    connection: Any,
    exchange: LegacyCandidateInput,
    *,
    fail_after_movie_merge: bool = False,
) -> int:
    """candidate DDL 뒤 observation과 SUCCESS ledger를 하나의 DML transaction에 쓴다."""
    validate_legacy_candidate_input(exchange)
    cursor = connection.cursor()
    try:
        # Snowflake DDL은 암묵적으로 commit하므로 업무 BEGIN보다 먼저 끝낸다.
        for statement in (part.strip() for part in DDL.split(";") if part.strip()):
            cursor.execute(statement)
        cursor.execute(
            "CREATE OR REPLACE TEMP TABLE LEGACY_CANDIDATE_INCOMING "
            "(PAYLOAD_TEXT STRING, RECORD_SHA256 STRING)"
        )
        if exchange.movies:
            cursor.executemany(
                "INSERT INTO LEGACY_CANDIDATE_INCOMING VALUES (%s,%s)",
                [_payload(row) for row in exchange.movies],
            )

        identity = (
            exchange.source_run_id,
            exchange.source_sha256,
            exchange.artifact_version,
            exchange.publication_revision,
            exchange.bundle_sha256,
        )

        def verify_persisted() -> None:
            cursor.execute(
                """SELECT COUNT(*) FROM POP_TALK_DW_DEV.CANDIDATE.MOVIE_OBSERVATIONS_RAW
                   WHERE SOURCE_RUN_ID=%s AND ARTIFACT_VERSION=%s""",
                (exchange.source_run_id, exchange.artifact_version),
            )
            if cursor.fetchone()[0] != len(exchange.movies):
                raise LegacyCandidateLoadError("Candidate persisted movie count mismatch")
            cursor.execute(
                """SELECT COUNT(*) FROM
                     (SELECT PARSE_JSON(PAYLOAD_TEXT) p, RECORD_SHA256
                        FROM LEGACY_CANDIDATE_INCOMING) s
                     LEFT JOIN POP_TALK_DW_DEV.CANDIDATE.MOVIE_OBSERVATIONS_RAW t
                       ON t.SOURCE_RUN_ID=%s AND t.ARTIFACT_VERSION=%s
                      AND t.CANONICAL_MOVIE_KEY=s.p:canonical_movie_key::STRING
                    WHERE t.RECORD_SHA256 IS NULL OR t.RECORD_SHA256<>s.RECORD_SHA256""",
                (exchange.source_run_id, exchange.artifact_version),
            )
            if cursor.fetchone()[0]:
                raise LegacyCandidateLoadError("Candidate persisted movie key/hash mismatch")

        cursor.execute("BEGIN")
        cursor.execute(
            """SELECT SOURCE_RUN_ID,SOURCE_SHA256,ARTIFACT_VERSION,
                      PUBLICATION_REVISION,SOURCE_BUNDLE_SHA256,STATUS,MOVIE_COUNT
                 FROM POP_TALK_DW_DEV.CANDIDATE.LEGACY_EXCHANGE_LOADS
                WHERE EXCHANGE_READY_KEY=%s""",
            (exchange.ready_key,),
        )
        previous = cursor.fetchone()
        if previous and tuple(previous[:5]) != identity:
            raise LegacyCandidateLoadError("Candidate load ledger identity conflict")
        if previous and previous[5] == "SUCCESS":
            if previous[6] != len(exchange.movies):
                raise LegacyCandidateLoadError("Candidate successful replay count conflict")
            verify_persisted()
            connection.commit()
            return len(exchange.movies)

        cursor.execute(
            """MERGE INTO POP_TALK_DW_DEV.CANDIDATE.LEGACY_EXCHANGE_LOADS t
               USING (SELECT %s k,%s r,%s s,%s a,%s p,%s b) v
                  ON t.EXCHANGE_READY_KEY=v.k
               WHEN MATCHED THEN UPDATE SET STATUS='RUNNING',UPDATED_AT=CURRENT_TIMESTAMP()
               WHEN NOT MATCHED THEN INSERT
                 (EXCHANGE_READY_KEY,SOURCE_RUN_ID,SOURCE_SHA256,ARTIFACT_VERSION,
                  PUBLICATION_REVISION,SOURCE_BUNDLE_SHA256,STATUS,UPDATED_AT)
               VALUES(v.k,v.r,v.s,v.a,v.p,v.b,'RUNNING',CURRENT_TIMESTAMP())""",
            (exchange.ready_key, *identity),
        )
        cursor.execute(
            """SELECT COUNT(*)
                 FROM POP_TALK_DW_DEV.CANDIDATE.MOVIE_OBSERVATIONS_RAW t
                 JOIN (SELECT PARSE_JSON(PAYLOAD_TEXT) p, RECORD_SHA256
                         FROM LEGACY_CANDIDATE_INCOMING) s
                   ON t.SOURCE_RUN_ID=%s AND t.ARTIFACT_VERSION=%s
                  AND t.CANONICAL_MOVIE_KEY=s.p:canonical_movie_key::STRING
                WHERE t.RECORD_SHA256<>s.RECORD_SHA256""",
            (exchange.source_run_id, exchange.artifact_version),
        )
        if cursor.fetchone()[0]:
            raise LegacyCandidateLoadError("Immutable candidate observation conflict")
        cursor.execute(
            """MERGE INTO POP_TALK_DW_DEV.CANDIDATE.MOVIE_OBSERVATIONS_RAW t
               USING (
                 SELECT %s SOURCE_RUN_ID,%s SOURCE_SHA256,%s ARTIFACT_VERSION,
                        %s PUBLICATION_REVISION,
                        p:canonical_movie_key::STRING CANONICAL_MOVIE_KEY,
                        p:source_observed_at::TIMESTAMP_TZ SOURCE_OBSERVED_AT,
                        p:source_observed_at_known::BOOLEAN SOURCE_OBSERVED_AT_KNOWN,
                        p:source_time_basis::STRING SOURCE_TIME_BASIS,
                        RECORD_SHA256,PARSE_JSON(PAYLOAD_TEXT) PAYLOAD,
                        %s EXCHANGE_READY_KEY
                   FROM (SELECT PAYLOAD_TEXT,PARSE_JSON(PAYLOAD_TEXT) p,RECORD_SHA256
                           FROM LEGACY_CANDIDATE_INCOMING)
               ) s
                  ON t.SOURCE_RUN_ID=s.SOURCE_RUN_ID
                 AND t.ARTIFACT_VERSION=s.ARTIFACT_VERSION
                 AND t.CANONICAL_MOVIE_KEY=s.CANONICAL_MOVIE_KEY
               WHEN NOT MATCHED THEN INSERT
                 (SOURCE_RUN_ID,SOURCE_SHA256,ARTIFACT_VERSION,PUBLICATION_REVISION,
                  CANONICAL_MOVIE_KEY,SOURCE_OBSERVED_AT,SOURCE_OBSERVED_AT_KNOWN,
                  SOURCE_TIME_BASIS,RECORD_SHA256,PAYLOAD,EXCHANGE_READY_KEY)
               VALUES(s.SOURCE_RUN_ID,s.SOURCE_SHA256,s.ARTIFACT_VERSION,
                  s.PUBLICATION_REVISION,s.CANONICAL_MOVIE_KEY,s.SOURCE_OBSERVED_AT,
                  s.SOURCE_OBSERVED_AT_KNOWN,s.SOURCE_TIME_BASIS,s.RECORD_SHA256,
                  s.PAYLOAD,s.EXCHANGE_READY_KEY)""",
            (
                exchange.source_run_id,
                exchange.source_sha256,
                exchange.artifact_version,
                exchange.publication_revision,
                exchange.ready_key,
            ),
        )
        if fail_after_movie_merge:
            raise LegacyCandidateLoadError("Injected candidate failure after movie merge")
        verify_persisted()
        cursor.execute(
            """UPDATE POP_TALK_DW_DEV.CANDIDATE.LEGACY_EXCHANGE_LOADS
                  SET STATUS='SUCCESS',MOVIE_COUNT=%s,UPDATED_AT=CURRENT_TIMESTAMP(),
                      COMPLETED_AT=CURRENT_TIMESTAMP()
                WHERE EXCHANGE_READY_KEY=%s""",
            (len(exchange.movies), exchange.ready_key),
        )
        connection.commit()
        return len(exchange.movies)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
