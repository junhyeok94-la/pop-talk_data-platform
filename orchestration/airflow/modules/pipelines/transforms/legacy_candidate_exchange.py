"""Databricks 검증 bundle을 candidate S3 Exchange v2에 불변 게시한다."""
from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from dataclasses import dataclass
from typing import Any

from pipelines.transforms.legacy_candidate_bridge import LegacyCandidateError
from pipelines.transforms.movie_bronze_silver import packed

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
SHA256 = re.compile(r"[0-9a-f]{64}")
REQUIRED_BUNDLE_DOCUMENTS = {
    "SUCCESS.json",
    "source/movies_final.json",
    "movie_bronze_silver.py",
    "silver_movies.jsonl",
    "silver_boxoffice.jsonl",
}


@dataclass(frozen=True)
class LegacyCandidatePublication:
    prefix: str
    ready_key: str
    source_run_id: str
    source_sha256: str
    artifact_version: str
    publication_revision: int
    movie_count: int
    boxoffice_count: int


def _put_immutable(
    client: Any, bucket: str, key: str, body: bytes, *, content_type: str
) -> None:
    """같은 key의 동일 bytes만 replay로 허용한다."""
    try:
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as error:
        status = getattr(error, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 404:
            raise
    else:
        if existing != body:
            raise LegacyCandidateError(f"Immutable candidate S3 object conflict: {key}")
        return
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            IfNoneMatch="*",
            ContentType=content_type,
            Metadata={"sha256": hashlib.sha256(body).hexdigest()},
        )
    except Exception as error:
        status = getattr(error, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 412:
            raise
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if existing != body:
            raise LegacyCandidateError(f"Concurrent candidate S3 conflict: {key}") from None


def _jsonl_rows(body: bytes) -> tuple[dict[str, Any], ...]:
    if not body:
        return ()
    if not body.endswith(b"\n"):
        raise LegacyCandidateError("Candidate JSONL must end with LF")
    try:
        rows = tuple(json.loads(line) for line in body.splitlines())
    except (UnicodeError, ValueError):
        raise LegacyCandidateError("Candidate Silver file is not valid JSONL") from None
    if any(not isinstance(row, dict) for row in rows):
        raise LegacyCandidateError("Candidate Silver JSONL row must be an object")
    return rows


def publish_legacy_candidate_exchange(
    s3_client: Any,
    *,
    bucket: str,
    archive: bytes,
    expected_archive_sha256: str,
    expected_source_sha256: str,
    expected_artifact_version: str,
    publication_revision: int,
) -> LegacyCandidatePublication:
    """bundle을 검증하고 두 data 파일 뒤 마지막에 READY를 쓴다."""
    if publication_revision < 1:
        raise LegacyCandidateError("Candidate publication revision must be positive")
    if not SHA256.fullmatch(expected_source_sha256) or not SHA256.fullmatch(expected_artifact_version):
        raise LegacyCandidateError("Candidate publication identity is invalid")
    if hashlib.sha256(archive).hexdigest() != expected_archive_sha256:
        raise LegacyCandidateError("Candidate archive digest mismatch")
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
            names = bundle.namelist()
            if len(names) != len(set(names)) or "bundle_index.json" not in names:
                raise LegacyCandidateError("Candidate bundle file set is invalid")
            index = json.loads(bundle.read("bundle_index.json"))
            documents = index.get("documents")
            if (
                not isinstance(documents, dict)
                or set(documents) != REQUIRED_BUNDLE_DOCUMENTS
                or set(names) != REQUIRED_BUNDLE_DOCUMENTS | {"bundle_index.json"}
            ):
                raise LegacyCandidateError("Candidate bundle differs from its index")
            for name, metadata in documents.items():
                body = bundle.read(name)
                if (
                    hashlib.sha256(body).hexdigest() != metadata.get("sha256")
                    or len(body) != metadata.get("bytes")
                ):
                    raise LegacyCandidateError(f"Candidate bundle checksum mismatch: {name}")
            movies = bundle.read("silver_movies.jsonl")
            boxoffice = bundle.read("silver_boxoffice.jsonl")
    except LegacyCandidateError:
        raise
    except Exception:
        raise LegacyCandidateError("Candidate bundle cannot be read") from None

    if (
        index.get("bundle_version") != 2
        or index.get("exchange_contract_version") != 2
        or index.get("source_sha256") != expected_source_sha256
        or index.get("artifact_version") != expected_artifact_version
    ):
        raise LegacyCandidateError("Candidate bundle identity mismatch")
    if index.get("manifest_key") != (
        f"manifests/legacy_snapshot/v1/sha256={expected_source_sha256}/SUCCESS.json"
    ) or index.get("source_key") != (
        f"raw/legacy_snapshot/v1/sha256={expected_source_sha256}/movies_final.json"
    ):
        raise LegacyCandidateError("Candidate bundle source lineage mismatch")
    movie_rows = _jsonl_rows(movies)
    if boxoffice != b"" or _jsonl_rows(boxoffice):
        raise LegacyCandidateError("Legacy candidate boxoffice must be empty bytes")
    expected = index.get("expected") or {}
    if len(movie_rows) != expected.get("silver_movies") or expected.get("silver_boxoffice") != 0:
        raise LegacyCandidateError("Candidate Silver row count mismatch")

    source_run_id = f"legacy:{expected_source_sha256}"
    if index.get("source_run_id") != source_run_id:
        raise LegacyCandidateError("Candidate source run identity mismatch")
    if b"".join(packed(row) + b"\n" for row in movie_rows) != movies:
        raise LegacyCandidateError("Candidate Silver JSONL is not canonical")
    movie_keys = [row.get("canonical_movie_key") for row in movie_rows]
    if any(not isinstance(key, str) or not key for key in movie_keys) or len(set(movie_keys)) != len(movie_keys):
        raise LegacyCandidateError("Candidate Silver movie key is invalid or duplicated")
    for row in movie_rows:
        if (
            row.get("source_run_id") != source_run_id
            or row.get("transform_version") != expected_artifact_version
            or row.get("ready_manifest_key") != index["manifest_key"]
            or row.get("source_observed_at") is not None
            or row.get("source_observed_at_known") is not False
            or row.get("source_time_basis") != "LEGACY_UNKNOWN"
        ):
            raise LegacyCandidateError("Candidate Silver v2 lineage/time contract mismatch")
    prefix = (
        "exchange/candidate/movie_legacy/v2/"
        f"source_sha256={expected_source_sha256}/"
        f"artifact_version={expected_artifact_version}/"
        f"publication_revision={publication_revision}"
    )
    files = {"movies.jsonl": movies, "boxoffice.jsonl": boxoffice}
    for name, body in files.items():
        _put_immutable(
            s3_client,
            bucket,
            f"{prefix}/{name}",
            body,
            content_type="application/x-ndjson",
        )
    ready = packed({
        "schema_version": 2,
        "exchange_contract_version": 2,
        "source_bundle_sha256": expected_archive_sha256,
        "source_manifest_key": index["manifest_key"],
        "source_object_key": index["source_key"],
        "source_run_id": source_run_id,
        "source_sha256": expected_source_sha256,
        "artifact_version": expected_artifact_version,
        "publication_revision": publication_revision,
        "files": {
            name: {
                "key": f"{prefix}/{name}",
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
                "row_count": len(movie_rows) if name == "movies.jsonl" else 0,
            }
            for name, body in files.items()
        },
    })
    ready_key = f"{prefix}/EXCHANGE_READY.json"
    _put_immutable(
        s3_client, bucket, ready_key, ready, content_type="application/json"
    )
    return LegacyCandidatePublication(
        prefix=prefix,
        ready_key=ready_key,
        source_run_id=source_run_id,
        source_sha256=expected_source_sha256,
        artifact_version=expected_artifact_version,
        publication_revision=publication_revision,
        movie_count=len(movie_rows),
        boxoffice_count=0,
    )
