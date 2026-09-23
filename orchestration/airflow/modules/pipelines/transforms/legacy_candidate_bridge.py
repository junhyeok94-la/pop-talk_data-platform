"""검증된 Legacy snapshot을 Databricks candidate용 불변 bundle로 만든다."""
from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from dataclasses import dataclass
from typing import Any

from pipelines.modeling.movie_baseline_reconciliation import (
    SnapshotContractError,
    validate_legacy_manifest_header,
    validate_legacy_snapshot,
)
from pipelines.transforms.movie_bronze_silver import (
    legacy_exchange_observations,
    packed,
    transform_legacy_snapshot,
)


class LegacyCandidateError(RuntimeError):
    """외부 응답 본문이나 자격 증명을 노출하지 않는 candidate 경계 오류."""


MANIFEST_PATTERN = re.compile(
    r"^manifests/legacy_snapshot/v1/sha256=([0-9a-f]{64})/SUCCESS[.]json$"
)


@dataclass(frozen=True)
class LegacyCandidateBundle:
    source_run_id: str
    source_sha256: str
    manifest_key: str
    landing_dir: str
    archive_path: str
    archive_sha256: str
    movie_count: int
    eligible_count: int
    excluded_count: int


def json_lines(rows: list[dict[str, Any]]) -> bytes:
    """canonical JSON 한 행과 LF 하나로 결정적 JSONL을 직렬화한다."""
    return b"".join(packed(row) + b"\n" for row in rows)


def deterministic_zip(documents: dict[str, bytes]) -> bytes:
    """입력 순서나 로컬 시각에 영향을 받지 않는 ZIP bytes를 반환한다."""
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, body in sorted(documents.items()):
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o600 << 16
            bundle.writestr(entry, body)
    return archive.getvalue()


def _read_s3(client: Any, bucket: str, key: str) -> bytes:
    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        raise LegacyCandidateError("Legacy candidate S3 input read failed") from None


def build_legacy_candidate_archive(
    *,
    bucket: str,
    manifest_key: str,
    manifest_body: bytes,
    source_body: bytes,
    transform_source: bytes,
    artifact_version: str,
) -> tuple[bytes, dict[str, Any]]:
    """manifest/source를 검증하고 원격 재계산 가능한 bundle을 생성한다."""
    match = MANIFEST_PATTERN.fullmatch(manifest_key)
    if not match:
        raise LegacyCandidateError("Legacy candidate manifest key is outside the allowlist")
    if not re.fullmatch(r"[0-9a-f]{64}", artifact_version):
        raise LegacyCandidateError("Legacy candidate artifact version is invalid")
    try:
        manifest = json.loads(manifest_body)
    except (UnicodeError, ValueError):
        raise LegacyCandidateError("Legacy candidate manifest is not valid JSON") from None
    if not isinstance(manifest, dict) or manifest.get("sha256") != match.group(1):
        raise LegacyCandidateError("Legacy candidate manifest path identity mismatch")
    expected_source_key = (
        f"raw/legacy_snapshot/v1/sha256={match.group(1)}/movies_final.json"
    )
    if manifest.get("key") != expected_source_key:
        raise LegacyCandidateError("Legacy candidate source key identity mismatch")
    try:
        records = validate_legacy_snapshot(manifest, source_body, expected_bucket=bucket)
        output = transform_legacy_snapshot(manifest, source_body)
        exchange = legacy_exchange_observations(
            output,
            manifest_key=manifest_key,
            transform_version=artifact_version,
        )
    except (SnapshotContractError, ValueError):
        raise LegacyCandidateError("Legacy candidate source contract failed") from None

    movie_body = json_lines(exchange["silver_movies"])
    boxoffice_body = b""
    documents = {
        "SUCCESS.json": manifest_body,
        "source/movies_final.json": source_body,
        "movie_bronze_silver.py": transform_source,
        "silver_movies.jsonl": movie_body,
        "silver_boxoffice.jsonl": boxoffice_body,
    }
    index = {
        "bundle_version": 2,
        "exchange_contract_version": 2,
        "bucket": bucket,
        "manifest_key": manifest_key,
        "source_key": manifest["key"],
        "source_sha256": manifest["sha256"],
        "source_run_id": f"legacy:{manifest['sha256']}",
        "artifact_version": artifact_version,
        "documents": {
            name: {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
            for name, body in sorted(documents.items())
        },
        "expected": {
            "bronze_objects": 1,
            "silver_movies": len(exchange["silver_movies"]),
            "silver_boxoffice": 0,
            "eligible_movies": output["quality"]["eligible_count"],
            "excluded_movies": output["quality"]["excluded_count"],
        },
    }
    documents["bundle_index.json"] = packed(index)
    return deterministic_zip(documents), index


def stage_legacy_candidate_bundle(
    s3_client: Any,
    files_client: Any,
    *,
    bucket: str,
    manifest_key: str,
    transform_source: bytes,
    artifact_version: str,
) -> LegacyCandidateBundle:
    """S3 입력을 검증한 뒤 candidate landing에 같은 archive만 허용한다."""
    if not MANIFEST_PATTERN.fullmatch(manifest_key):
        raise LegacyCandidateError("Legacy candidate manifest key is outside the allowlist")
    manifest_body = _read_s3(s3_client, bucket, manifest_key)
    try:
        manifest = json.loads(manifest_body)
    except (UnicodeError, ValueError):
        raise LegacyCandidateError("Legacy candidate manifest is not valid JSON") from None
    if not isinstance(manifest, dict) or not isinstance(manifest.get("key"), str):
        raise LegacyCandidateError("Legacy candidate manifest shape is invalid")
    match = MANIFEST_PATTERN.fullmatch(manifest_key)
    try:
        validate_legacy_manifest_header(manifest, expected_bucket=bucket)
    except SnapshotContractError:
        raise LegacyCandidateError("Legacy candidate manifest contract failed") from None
    if (
        not match
        or manifest.get("sha256") != match.group(1)
        or manifest["key"]
        != f"raw/legacy_snapshot/v1/sha256={match.group(1)}/movies_final.json"
    ):
        raise LegacyCandidateError("Legacy candidate manifest/source path identity mismatch")
    source_body = _read_s3(s3_client, bucket, manifest["key"])
    archive, index = build_legacy_candidate_archive(
        bucket=bucket,
        manifest_key=manifest_key,
        manifest_body=manifest_body,
        source_body=source_body,
        transform_source=transform_source,
        artifact_version=artifact_version,
    )
    source_sha = index["source_sha256"]
    landing = (
        "/Volumes/workspace/bronze/landing/movie_legacy_candidate/"
        f"{source_sha}/{artifact_version}"
    )
    files_client.mkdir(landing)
    archive_path = f"{landing}/bundle.zip"
    archive_sha = files_client.put_immutable(archive_path, archive)
    if archive_sha != hashlib.sha256(archive).hexdigest():
        raise LegacyCandidateError("Databricks landing archive digest mismatch")
    return LegacyCandidateBundle(
        source_run_id=index["source_run_id"],
        source_sha256=source_sha,
        manifest_key=manifest_key,
        landing_dir=landing,
        archive_path=archive_path,
        archive_sha256=archive_sha,
        movie_count=index["expected"]["silver_movies"],
        eligible_count=index["expected"]["eligible_movies"],
        excluded_count=index["expected"]["excluded_movies"],
    )
