"""Legacy candidate DAG의 외부 작업을 Airflow와 분리해 제공한다."""
from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from typing import Any, TypedDict

import requests


ARTIFACT_DEPENDENCIES = (
    "pipelines/databricks/04_movie_legacy_candidate.py",
    "pipelines/modeling/movie_baseline_reconciliation.py",
    "pipelines/orchestration/movie_legacy_candidate_runtime.py",
    "pipelines/transforms/databricks_bridge.py",
    "pipelines/transforms/legacy_candidate_bridge.py",
    "pipelines/transforms/legacy_candidate_exchange.py",
    "pipelines/transforms/legacy_candidate_snowflake.py",
    "pipelines/transforms/movie_bronze_silver.py",
)


class CandidateNotebook(TypedDict):
    notebook_path: str
    artifact_version: str


class CandidateStaged(TypedDict):
    source_run_id: str
    source_sha256: str
    manifest_key: str
    landing_dir: str
    archive_path: str
    archive_sha256: str
    artifact_version: str
    publication_revision: int
    processing_attempt: int
    submit_token: str
    movie_count: int
    eligible_count: int
    excluded_count: int


class CandidatePublished(TypedDict):
    prefix: str
    ready_key: str
    source_run_id: str
    source_sha256: str
    artifact_version: str
    publication_revision: int
    movie_count: int
    boxoffice_count: int


class CandidateLoaded(TypedDict):
    exchange_ready_key: str
    source_run_id: str
    source_sha256: str
    artifact_version: str
    publication_revision: int
    movie_count: int


def artifact_sources(project_root: Path) -> dict[str, bytes]:
    """원격 결과와 bundle 검증에 영향을 주는 정확한 source bytes를 읽는다."""
    return {
        relative: (project_root / relative).read_bytes()
        for relative in ARTIFACT_DEPENDENCIES
    }


def artifact_version(sources: dict[str, bytes]) -> str:
    """상대 경로와 bytes를 모두 포함한 결정적 artifact SHA-256을 계산한다."""
    if set(sources) != set(ARTIFACT_DEPENDENCIES):
        raise RuntimeError("Legacy candidate artifact source set is incomplete")
    body = b"".join(
        relative.encode("utf-8") + b"\0" + sources[relative] + b"\0"
        for relative in sorted(ARTIFACT_DEPENDENCIES)
    )
    return hashlib.sha256(body).hexdigest()


def _same_artifact(expected: str, actual: str) -> None:
    if expected != actual:
        raise RuntimeError("Legacy candidate artifact changed during the DAG run")


def deploy_notebook(
    *, project_root: Path, host: str, token: str, notebook_directory: str
) -> CandidateNotebook:
    """digest 경로의 notebook은 최초 생성 후 동일 bytes만 재사용한다."""
    sources = artifact_sources(project_root)
    version = artifact_version(sources)
    notebook_source = sources["pipelines/databricks/04_movie_legacy_candidate.py"]
    path = f"{notebook_directory.rstrip('/')}/{version}"
    headers = {"Authorization": f"Bearer {token}"}
    directory = requests.post(
        host.rstrip("/") + "/api/2.0/workspace/mkdirs",
        headers=headers,
        json={"path": notebook_directory},
        timeout=(10, 60),
    )
    if directory.status_code not in (200, 201):
        raise RuntimeError(
            f"Databricks candidate directory creation failed with HTTP {directory.status_code}"
        )
    status = requests.get(
        host.rstrip("/") + "/api/2.0/workspace/get-status",
        headers=headers, params={"path": path}, timeout=(10, 60),
    )
    if status.status_code == 200:
        exported = requests.get(
            host.rstrip("/") + "/api/2.0/workspace/export",
            headers=headers,
            params={"path": path, "format": "SOURCE"},
            timeout=(10, 60),
        )
        if exported.status_code != 200:
            raise RuntimeError(
                f"Databricks candidate notebook verification failed with HTTP {exported.status_code}"
            )
        try:
            remote = base64.b64decode(exported.json()["content"], validate=True)
        except Exception:
            raise RuntimeError("Databricks candidate notebook export is invalid") from None
        if remote != notebook_source:
            raise RuntimeError("Immutable Databricks candidate notebook content conflict")
    elif status.status_code == 404:
        imported = requests.post(
            host.rstrip("/") + "/api/2.0/workspace/import",
            headers=headers,
            json={
                "path": path,
                "format": "SOURCE",
                "language": "PYTHON",
                "content": base64.b64encode(notebook_source).decode("ascii"),
                "overwrite": False,
            },
            timeout=(10, 60),
        )
        if imported.status_code not in (200, 201):
            raise RuntimeError(
                f"Databricks candidate notebook import failed with HTTP {imported.status_code}"
            )
    else:
        raise RuntimeError(
            f"Databricks candidate notebook status failed with HTTP {status.status_code}"
        )
    return {"notebook_path": path, "artifact_version": version}


def stage_bundle(
    *,
    project_root: Path,
    s3_client: Any,
    databricks_host: str,
    databricks_token: str,
    bucket: str,
    manifest_key: str,
    publication_revision: int,
    processing_attempt: int,
    deployed: CandidateNotebook,
) -> CandidateStaged:
    """정확한 Legacy snapshot을 검증해 candidate Volume에 불변 staging한다."""
    from pipelines.transforms.databricks_bridge import DatabricksFilesClient
    from pipelines.transforms.legacy_candidate_bridge import stage_legacy_candidate_bundle

    sources = artifact_sources(project_root)
    version = artifact_version(sources)
    _same_artifact(deployed["artifact_version"], version)
    with requests.Session() as session:
        files = DatabricksFilesClient(
            databricks_host, databricks_token, session=session
        )
        result = stage_legacy_candidate_bundle(
            s3_client,
            files,
            bucket=bucket,
            manifest_key=manifest_key,
            transform_source=sources["pipelines/transforms/movie_bronze_silver.py"],
            artifact_version=version,
        )
    token_input = (
        f"{manifest_key}|{result.source_sha256}|{version}|"
        f"{publication_revision}|{processing_attempt}"
    )
    return {
        **result.__dict__,
        "artifact_version": version,
        "publication_revision": publication_revision,
        "processing_attempt": processing_attempt,
        "submit_token": hashlib.sha256(token_input.encode("utf-8")).hexdigest(),
    }


def publish_exchange(
    *,
    project_root: Path,
    s3_client: Any,
    databricks_host: str,
    databricks_token: str,
    bucket: str,
    staged: CandidateStaged,
) -> CandidatePublished:
    """고정 artifact의 원격 bundle을 읽어 candidate S3 prefix에만 게시한다."""
    from pipelines.transforms.databricks_bridge import DatabricksFilesClient
    from pipelines.transforms.legacy_candidate_exchange import (
        publish_legacy_candidate_exchange,
    )

    version = artifact_version(artifact_sources(project_root))
    _same_artifact(staged["artifact_version"], version)
    with requests.Session() as session:
        files = DatabricksFilesClient(
            databricks_host, databricks_token, session=session
        )
        archive = files.get(staged["archive_path"])
    result = publish_legacy_candidate_exchange(
        s3_client,
        bucket=bucket,
        archive=archive,
        expected_archive_sha256=staged["archive_sha256"],
        expected_source_sha256=staged["source_sha256"],
        expected_artifact_version=version,
        publication_revision=staged["publication_revision"],
    )
    return result.__dict__


def load_snowflake(
    *,
    project_root: Path,
    s3_client: Any,
    snowflake_connection: Any,
    bucket: str,
    published: CandidatePublished,
    fail_after_movie_merge: bool = False,
) -> CandidateLoaded:
    """candidate READY를 재검증해 Snowflake CANDIDATE에만 적재한다."""
    current_artifact = artifact_version(artifact_sources(project_root))
    _same_artifact(published["artifact_version"], current_artifact)
    from pipelines.transforms.legacy_candidate_snowflake import (
        load_legacy_candidate,
        read_legacy_candidate_exchange,
    )

    exchange = read_legacy_candidate_exchange(
        s3_client, bucket=bucket, ready_key=published["ready_key"]
    )
    if (
        exchange.source_run_id != published["source_run_id"]
        or exchange.source_sha256 != published["source_sha256"]
        or exchange.artifact_version != published["artifact_version"]
        or exchange.publication_revision != published["publication_revision"]
        or len(exchange.movies) != published["movie_count"]
        or published["boxoffice_count"] != 0
    ):
        raise RuntimeError("Published candidate metadata differs from READY content")
    count = load_legacy_candidate(
        snowflake_connection,
        exchange,
        fail_after_movie_merge=fail_after_movie_merge,
    )
    return {
        "exchange_ready_key": exchange.ready_key,
        "source_run_id": exchange.source_run_id,
        "source_sha256": exchange.source_sha256,
        "artifact_version": exchange.artifact_version,
        "publication_revision": exchange.publication_revision,
        "movie_count": count,
    }
