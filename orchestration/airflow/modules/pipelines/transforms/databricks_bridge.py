"""Verified S3 raw manifest transfer to a Databricks Unity Catalog volume."""
from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import quote

from pipelines.transforms.movie_bronze_silver import exchange_observations, packed, transform_run


class BridgeError(RuntimeError):
    """Sanitized bridge failure; credentials and response bodies are never included."""


def require_same_artifact(deployed_version: str, staged_version: str) -> None:
    """Fail closed if source files changed between deploy and staging tasks."""
    if deployed_version != staged_version:
        raise BridgeError(
            "Processing artifact changed after notebook deployment; start a new DAG run"
        )


@dataclass(frozen=True)
class StagedBundle:
    run_id: str
    landing_dir: str
    index_path: str
    object_count: int
    movie_count: int
    archive_sha256: str


class DatabricksFilesClient:
    def __init__(self, host: str, token: str, *, session: Any):
        if not host or not token:
            raise BridgeError("Databricks host and token are required")
        self.host, self.token, self.session = host.rstrip("/"), token, session

    def mkdir(self, volume_path: str) -> None:
        endpoint = "/api/2.0/fs/directories" + quote(volume_path, safe="/")
        response = self.session.request("PUT", self.host + endpoint,
                                        headers={"Authorization": f"Bearer {self.token}"},
                                        timeout=(10, 60))
        if response.status_code not in (200, 201, 204, 409):
            raise BridgeError(f"Databricks directory creation failed with HTTP {response.status_code}")

    def put_immutable(self, volume_path: str, body: bytes) -> str:
        endpoint = "/api/2.0/fs/files" + quote(volume_path, safe="/")
        headers = {"Authorization": f"Bearer {self.token}"}
        existing = self.session.request("GET", self.host + endpoint, headers=headers, timeout=(10, 60))
        if existing.status_code == 200:
            if existing.content != body:
                raise BridgeError("Immutable Databricks landing file conflict")
            return hashlib.sha256(body).hexdigest()
        if existing.status_code != 404:
            raise BridgeError(f"Databricks file read failed with HTTP {existing.status_code}")
        uploaded = self.session.request("PUT", self.host + endpoint,
                                        headers={**headers, "Content-Type": "application/octet-stream"},
                                        params={"overwrite": "false"}, data=body, timeout=(10, 60))
        if uploaded.status_code not in (200, 201, 204):
            raise BridgeError(f"Databricks file upload failed with HTTP {uploaded.status_code}")
        return hashlib.sha256(body).hexdigest()

    def get(self, volume_path: str) -> bytes:
        endpoint = "/api/2.0/fs/files" + quote(volume_path, safe="/")
        response = self.session.request(
            "GET", self.host + endpoint,
            headers={"Authorization": f"Bearer {self.token}"}, timeout=(10, 120),
        )
        if response.status_code != 200:
            raise BridgeError(f"Databricks file read failed with HTTP {response.status_code}")
        return response.content


def json_lines(rows: list[dict[str, Any]]) -> bytes:
    return b"".join(packed(row) + b"\n" for row in rows)


def _s3_json(client: Any, bucket: str, key: str) -> tuple[dict[str, Any], bytes]:
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        value = json.loads(body)
    except (UnicodeError, ValueError):
        raise BridgeError("S3 input is not valid UTF-8 JSON") from None
    if not isinstance(value, dict):
        raise BridgeError("S3 control/raw document must be an object")
    return value, body


def stage_daily_bundle(
    s3_client: Any,
    files_client: DatabricksFilesClient,
    *,
    bucket: str,
    ready_key: str,
    transform_source: bytes,
    artifact_version: str,
) -> StagedBundle:
    """Validate a complete daily run, then copy its exact documents immutably."""
    ready, ready_body = _s3_json(s3_client, bucket, ready_key)
    success_key = str(ready.get("raw_success_manifest") or "")
    if not success_key:
        raise BridgeError("DAILY_READY has no raw SUCCESS reference")
    success, success_body = _s3_json(s3_client, bucket, success_key)
    stages: dict[str, dict[str, Any]] = {}
    stage_bodies: dict[str, bytes] = {}
    for name, key in (success.get("stage_manifests") or {}).items():
        stages[name], stage_bodies[name] = _s3_json(s3_client, bucket, key)
    references = {ref["key"]: ref for stage in stages.values()
                  for ref in stage.get("objects") or []}
    raw, raw_bodies = {}, {}
    for key in sorted(references):
        raw[key], raw_bodies[key] = _s3_json(s3_client, bucket, key)
    # The exact same pure contract used by Databricks is the transfer gate.
    output = transform_run(ready, success, stages, raw)
    exchange = exchange_observations(output, raw, ready_key, artifact_version)

    run_id = str(ready["run_id"])
    if len(artifact_version) != 64 or any(c not in "0123456789abcdef" for c in artifact_version):
        raise BridgeError("Processing artifact version must be a SHA-256 hex digest")
    landing = f"/Volumes/workspace/bronze/landing/movie_api_daily/{run_id}/{artifact_version}"
    files_client.mkdir(landing)
    bundle_documents: dict[str, bytes] = {
        "DAILY_READY.json": ready_body,
        "SUCCESS.json": success_body,
    }
    for name in sorted(stage_bodies):
        bundle_documents[f"stage_{name}.json"] = stage_bodies[name]
    raw_index: dict[str, str] = {}
    for number, key in enumerate(sorted(raw_bodies)):
        relative = f"raw/{number:05d}.json"
        bundle_documents[relative] = raw_bodies[key]
        raw_index[key] = relative
    bundle_documents["movie_bronze_silver.py"] = transform_source
    bundle_documents["silver_movies.jsonl"] = json_lines(exchange["silver_movies"])
    bundle_documents["silver_boxoffice.jsonl"] = json_lines(exchange["silver_boxoffice"])
    index = {
        "bundle_version": 1,
        "bucket": bucket,
        "ready_key": ready_key,
        "run_id": run_id,
        "artifact_version": artifact_version,
        "documents": {name: hashlib.sha256(body).hexdigest()
                      for name, body in sorted(bundle_documents.items())},
        "raw_index": raw_index,
        "stage_files": {name: f"stage_{name}.json" for name in sorted(stages)},
        "transform_source_sha256": hashlib.sha256(transform_source).hexdigest(),
        "expected": {"bronze_objects": len(output["bronze"]),
                     "silver_movies": len(output["silver_movies"]),
                     "silver_boxoffice": len(output["silver_boxoffice"])},
    }
    index_body = packed(index)
    bundle_documents["bundle_index.json"] = index_body
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, body in sorted(bundle_documents.items()):
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o600 << 16
            bundle.writestr(entry, body)
    archive_path = f"{landing}/bundle.zip"
    archive_body = archive.getvalue()
    archive_sha256 = files_client.put_immutable(archive_path, archive_body)
    return StagedBundle(run_id, landing, archive_path,
                        len(raw), len(output["silver_movies"]), archive_sha256)
