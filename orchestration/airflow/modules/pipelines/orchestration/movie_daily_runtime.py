"""영화 Silver 변환부터 Snowflake 적재까지의 실행 작업을 제공한다.

이 모듈은 의도적으로 Airflow를 import하지 않는다. DAG가 인증된 client와 DB 연결을
전달하고, 이 함수들은 실제 작업과 태스크 사이의 작은 metadata 계약을 담당한다.
"""
from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

import requests


TRANSFORM_DEPENDENCIES = (
    "pipelines/transforms/databricks_bridge.py",
    "pipelines/transforms/exchange_publisher.py",
    "pipelines/transforms/movie_bronze_silver.py",
    "pipelines/databricks/03_movie_bronze_silver_daily.py",
    "pipelines/orchestration/movie_daily_runtime.py",
)


class DeployedNotebook(TypedDict):
    notebook_path: str
    artifact_version: str


class StagedRun(TypedDict):
    run_id: str
    ready_key: str
    landing_dir: str
    index_path: str
    object_count: int
    movie_count: int
    artifact_version: str
    archive_sha256: str
    publication_revision: int
    processing_attempt: int
    submit_token: str


class PublishedExchange(TypedDict):
    prefix: str
    ready_key: str
    movie_count: int
    boxoffice_count: int


class LoadedExchange(TypedDict):
    exchange_ready_key: str
    source_run_id: str
    artifact_version: str
    publication_revision: int
    movie_count: int
    boxoffice_count: int


@dataclass(frozen=True)
class TransformArtifact:
    """하나의 artifact version이 대표하는 정확한 로컬 소스 묶음."""

    transform_source: bytes
    notebook_source: bytes
    version: str


def transform_artifact(project_root: Path) -> TransformArtifact:
    """Silver 게시 결과에 영향을 주는 전이적 로컬 모듈을 모두 hash한다."""
    sources = {
        relative: (project_root / relative).read_bytes()
        for relative in TRANSFORM_DEPENDENCIES
    }
    digest_input = b"".join(
        relative.encode("utf-8") + b"\0" + sources[relative] + b"\0"
        for relative in TRANSFORM_DEPENDENCIES
    )
    return TransformArtifact(
        transform_source=sources["pipelines/transforms/movie_bronze_silver.py"],
        notebook_source=sources[
            "pipelines/databricks/03_movie_bronze_silver_daily.py"
        ],
        version=hashlib.sha256(digest_input).hexdigest(),
    )


def dbt_artifact(project_root: Path) -> str:
    """실행 가능한 dbt SQL과 YAML의 결정적인 식별자를 반환한다."""
    from pipelines.orchestration.dbt_deployment import model_version

    return model_version(project_root / "pipelines" / "dbt")


def deploy_notebook(
    *, project_root: Path, host: str, token: str, notebook_directory: str
) -> DeployedNotebook:
    """버전화된 노트북을 배포한다. 같은 버전 재시도는 동일 경로를 갱신한다."""
    artifact = transform_artifact(project_root)
    notebook_path = f"{notebook_directory}/{artifact.version}"
    headers = {"Authorization": f"Bearer {token}"}

    directory = requests.post(
        host.rstrip("/") + "/api/2.0/workspace/mkdirs",
        headers=headers,
        json={"path": notebook_directory},
        timeout=(10, 60),
    )
    if directory.status_code not in (200, 201):
        raise RuntimeError(
            "Databricks workspace directory creation failed "
            f"with HTTP {directory.status_code}"
        )

    response = requests.post(
        host.rstrip("/") + "/api/2.0/workspace/import",
        headers=headers,
        json={
            "path": notebook_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "content": base64.b64encode(artifact.notebook_source).decode("ascii"),
            "overwrite": True,
        },
        timeout=(10, 60),
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Databricks notebook deployment failed with HTTP {response.status_code}"
        )
    return {
        "notebook_path": notebook_path,
        "artifact_version": artifact.version,
    }


def stage_bundle(
    *,
    project_root: Path,
    s3_client: Any,
    databricks_host: str,
    databricks_token: str,
    bucket: str,
    ready_key: str,
    expected_raw_run_id: str,
    publication_revision: int,
    processing_attempt: int,
    deployed: DeployedNotebook,
) -> StagedRun:
    """Raw 실행 하나를 검증하고 정확한 bundle을 Databricks에 불변 staging한다."""
    from pipelines.transforms.databricks_bridge import (
        DatabricksFilesClient,
        require_same_artifact,
        stage_daily_bundle,
    )

    artifact = transform_artifact(project_root)
    require_same_artifact(deployed["artifact_version"], artifact.version)
    with requests.Session() as session:
        files = DatabricksFilesClient(
            databricks_host,
            databricks_token,
            session=session,
        )
        result = stage_daily_bundle(
            s3_client,
            files,
            bucket=bucket,
            ready_key=ready_key,
            transform_source=artifact.transform_source,
            artifact_version=artifact.version,
        )
    if result.run_id != expected_raw_run_id:
        raise RuntimeError("Asset lineage raw_run_id와 DAILY_READY 본문이 다릅니다")
    submit_token = hashlib.sha256(
        (
            f"{ready_key}|{artifact.version}|{publication_revision}|"
            f"{processing_attempt}"
        ).encode("utf-8")
    ).hexdigest()
    return {
        "run_id": result.run_id,
        "ready_key": ready_key,
        "landing_dir": result.landing_dir,
        "index_path": result.index_path,
        "object_count": result.object_count,
        "movie_count": result.movie_count,
        "artifact_version": artifact.version,
        "archive_sha256": result.archive_sha256,
        "publication_revision": publication_revision,
        "processing_attempt": processing_attempt,
        "submit_token": submit_token,
    }


def publish_exchange(
    *,
    project_root: Path,
    s3_client: Any,
    databricks_host: str,
    databricks_token: str,
    bucket: str,
    staged: StagedRun,
) -> PublishedExchange:
    """Databricks 출력을 검증하고 불변 S3 파일과 마지막 READY를 게시한다."""
    from pipelines.transforms.databricks_bridge import (
        DatabricksFilesClient,
        require_same_artifact,
    )
    from pipelines.transforms.exchange_publisher import publish_exchange_bundle

    current_artifact = transform_artifact(project_root)
    require_same_artifact(staged["artifact_version"], current_artifact.version)
    with requests.Session() as session:
        files = DatabricksFilesClient(
            databricks_host,
            databricks_token,
            session=session,
        )
        archive = files.get(staged["index_path"])
    result = publish_exchange_bundle(
        s3_client,
        bucket=bucket,
        archive=archive,
        expected_archive_sha256=staged["archive_sha256"],
        expected_run_id=staged["run_id"],
        expected_artifact_version=staged["artifact_version"],
        publication_revision=int(staged["publication_revision"]),
    )
    return {
        "prefix": result.prefix,
        "ready_key": result.ready_key,
        "movie_count": result.movie_count,
        "boxoffice_count": result.boxoffice_count,
    }


def load_snowflake(
    *,
    s3_client: Any,
    snowflake_connection: Any,
    bucket: str,
    exchange: PublishedExchange,
    fail_after_movie_merge: bool,
) -> LoadedExchange:
    """S3 Exchange 게시 하나를 단일 DML 트랜잭션으로 Snowflake에 적재한다."""
    from pipelines.transforms.snowflake_exchange_loader import load_exchange, read_exchange

    source = read_exchange(
        s3_client,
        bucket=bucket,
        ready_key=exchange["ready_key"],
    )
    movie_count, boxoffice_count = load_exchange(
        snowflake_connection,
        source,
        fail_after_movie_merge=fail_after_movie_merge,
    )
    return {
        "exchange_ready_key": source.ready_key,
        "source_run_id": source.source_run_id,
        "artifact_version": source.artifact_version,
        "publication_revision": source.publication_revision,
        "movie_count": movie_count,
        "boxoffice_count": boxoffice_count,
    }

