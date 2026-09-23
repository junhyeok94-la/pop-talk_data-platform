"""Publish a Databricks-validated Silver bundle to immutable S3 exchange keys."""
from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass
from typing import Any

from pipelines.transforms.databricks_bridge import BridgeError
from pipelines.transforms.movie_bronze_silver import packed


@dataclass(frozen=True)
class ExchangePublication:
    prefix: str
    ready_key: str
    movie_count: int
    boxoffice_count: int


def _put_s3_immutable(client: Any, bucket: str, key: str, body: bytes) -> None:
    try:
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as error:
        status = getattr(error, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 404:
            raise
    else:
        if existing != body:
            raise BridgeError(f"Immutable S3 exchange object conflict: {key}")
        return
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*",
                          ContentType="application/x-ndjson")
    except Exception as error:
        status = getattr(error, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 412:
            raise
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if existing != body:
            raise BridgeError(f"Concurrent immutable S3 exchange conflict: {key}") from None


def _jsonl_count(body: bytes) -> int:
    count = 0
    for line in body.splitlines():
        if line.strip():
            value = json.loads(line)
            if not isinstance(value, dict):
                raise BridgeError("Silver JSONL row must be an object")
            count += 1
    return count


def publish_exchange_bundle(
    s3_client: Any,
    *,
    bucket: str,
    archive: bytes,
    expected_archive_sha256: str,
    expected_run_id: str,
    expected_artifact_version: str,
    publication_revision: int,
) -> ExchangePublication:
    """Verify the immutable bundle and publish data first, READY marker last."""
    if publication_revision < 1:
        raise BridgeError("Publication revision must be positive")
    archive_sha256 = hashlib.sha256(archive).hexdigest()
    if archive_sha256 != expected_archive_sha256:
        raise BridgeError("Staged bundle digest differs from approved archive")
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        index = json.loads(bundle.read("bundle_index.json"))
        if index.get("bundle_version") != 1:
            raise BridgeError("Unsupported Silver bundle version")
        if index.get("run_id") != expected_run_id:
            raise BridgeError("Staged bundle run identity mismatch")
        if index.get("artifact_version") != expected_artifact_version:
            raise BridgeError("Staged bundle artifact identity mismatch")
        documents = index.get("documents")
        required = {"silver_movies.jsonl", "silver_boxoffice.jsonl"}
        if not isinstance(documents, dict) or not required.issubset(documents):
            raise BridgeError("Silver bundle is missing required document checksums")
        archive_names = bundle.namelist()
        if len(archive_names) != len(set(archive_names)) or set(archive_names) != set(documents) | {"bundle_index.json"}:
            raise BridgeError("Silver bundle file set differs from its checksum contract")
        for name, expected in documents.items():
            if hashlib.sha256(bundle.read(name)).hexdigest() != expected:
                raise BridgeError(f"Bundle checksum mismatch before S3 publish: {name}")
        movies = bundle.read("silver_movies.jsonl")
        boxoffice = bundle.read("silver_boxoffice.jsonl")

    movie_count, boxoffice_count = _jsonl_count(movies), _jsonl_count(boxoffice)
    expected = index["expected"]
    if movie_count != expected["silver_movies"] or boxoffice_count != expected["silver_boxoffice"]:
        raise BridgeError("Silver exchange row count differs from bundle contract")

    run_id, artifact_version = expected_run_id, expected_artifact_version
    prefix = (
        "exchange/movie_silver/v1/"
        f"source_run_id={run_id}/artifact_version={artifact_version}/"
        f"publication_revision={publication_revision}"
    )
    files = {
        "movies.jsonl": movies,
        "boxoffice.jsonl": boxoffice,
    }
    for name, body in files.items():
        _put_s3_immutable(s3_client, bucket, f"{prefix}/{name}", body)

    ready = packed({
        "schema_version": 1,
        "exchange_contract_version": 1,
        "source_bundle_sha256": archive_sha256,
        "source_run_id": run_id,
        "source_ready_key": index["ready_key"],
        "artifact_version": artifact_version,
        "publication_revision": publication_revision,
        "files": {
            name: {"key": f"{prefix}/{name}", "sha256": hashlib.sha256(body).hexdigest(),
                   "row_count": _jsonl_count(body)}
            for name, body in files.items()
        },
    })
    ready_key = f"{prefix}/EXCHANGE_READY.json"
    _put_s3_immutable(s3_client, bucket, ready_key, ready)
    return ExchangePublication(prefix, ready_key, movie_count, boxoffice_count)
