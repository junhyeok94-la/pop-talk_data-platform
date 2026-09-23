"""Versioned execution environments; credentials remain in Airflow Connections."""

import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from airflow_workbench.shared.contracts import Contract

KINDS = ("diagnostic", "llm_sft", "embedding_contrastive")


class Environment(Contract):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,47}$")
    name: str = Field(min_length=1, max_length=100)
    version: int = Field(default=0, ge=0)
    backend: Literal["http", "kubernetes"] = "http"
    connection_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,128}$")
    dag_prefix: str = Field(pattern=r"^[a-z][a-z0-9_]{0,170}$")
    pool: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")
    pool_slots: int = Field(default=1, ge=1, le=128)
    capacity: int = Field(default=1, ge=1, le=128)
    max_active_runs: int = Field(default=1, ge=1, le=32)
    timeout_seconds: int = Field(default=21600, ge=120, le=604800)
    poll_seconds: int = Field(default=10, ge=3, le=120)
    workloads: list[Literal["diagnostic", "llm_sft", "embedding_contrastive"]] = Field(
        default_factory=lambda: list(KINDS), min_length=1, max_length=3
    )
    resource_profiles: dict[str, str] = Field(
        default_factory=lambda: {
            "diagnostic": "diagnostic",
            "qlora": "qlora_8b",
            "lora": "lora_small",
            "full": "embedding_small",
        },
        max_length=10,
    )
    image: str = Field(default="", max_length=300, pattern=r"^[a-zA-Z0-9./:@_-]*$")
    namespace: str = Field(default="default", pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    service_account: str = Field(
        default="default", pattern=r"^[a-z0-9][a-z0-9-]{0,62}$"
    )
    cpu: str = Field(default="2", pattern=r"^[1-9][0-9]*(?:m)?$")
    memory: str = Field(default="8Gi", pattern=r"^[1-9][0-9]*(?:Mi|Gi)$")
    gpu: int = Field(default=1, ge=0, le=16)
    node_selector: dict[str, str] = Field(default_factory=dict, max_length=10)

    @model_validator(mode="after")
    def valid_target(self):
        if self.pool_slots > self.capacity:
            raise ValueError("작업 slot은 pool 용량보다 클 수 없습니다.")
        if len(set(self.workloads)) != len(self.workloads):
            raise ValueError("workload가 중복됩니다.")
        if self.backend == "kubernetes" and (
            not self.image
            or ":" not in self.image.rsplit("/", 1)[-1]
            or self.image.endswith(":latest")
        ):
            raise ValueError(
                "Kubernetes 실행 이미지는 고정 tag 또는 digest가 필요합니다."
            )
        return self

    def dag_ids(self):
        from airflow_workbench.shared.execution_contract import dag_ids

        return dag_ids(self.dag_prefix, self.workloads)

    def fingerprint(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()[:16]


def configured():
    from airflow_workbench.shared.database import Store

    with Store().connection() as db:
        return [
            Environment.model_validate_json(r[0])
            for r in db.execute(
                "SELECT value FROM documents WHERE key LIKE 'environment:%' ORDER BY key"
            )
        ]


def resolve(environment_id=""):
    from airflow_workbench.shared.database import NotFound

    values = configured()
    if not environment_id and len(values) == 1:
        return values[0]
    for value in values:
        if value.id == environment_id:
            return value
    raise NotFound("실행 환경을 등록하고 선택하세요.")


def save(spec):
    from airflow_workbench.shared.database import Conflict, Store

    with Store().connection() as db:
        db.lock("environments")
        rows = db.execute(
            "SELECT value FROM documents WHERE key LIKE 'environment:%'"
        ).fetchall()
        previous = None
        for row in rows:
            other = Environment.model_validate_json(row[0])
            if other.id == spec.id:
                previous = other
            elif other.dag_prefix == spec.dag_prefix:
                raise Conflict("다른 실행 환경이 같은 DAG prefix를 사용하고 있습니다.")
            elif other.pool == spec.pool and other.capacity != spec.capacity:
                raise Conflict("같은 pool을 공유하는 환경은 용량이 같아야 합니다.")
        if spec.version != (previous.version if previous else 0):
            raise Conflict("실행 환경이 변경되었습니다. 다시 불러오세요.")
        saved = spec.model_copy(update={"version": spec.version + 1})
        db.execute(
            "INSERT INTO documents VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=excluded.version",
            ("environment:" + spec.id, saved.model_dump_json(), saved.version),
        )
    return saved


def dag_source(spec):
    """Deterministic, secret-free DAG bundle. No DB/Connection access at parse time."""
    return (
        '"""Model Lab environment bundle. Generated settings; execution is external."""\n'
        "import json\nfrom airflow_workbench.model_lab.dag_factory import build_dags\n"
        f"ENVIRONMENT = json.loads({spec.model_dump_json()!r})\n"
        "for workflow in build_dags(ENVIRONMENT):\n    globals()[workflow.dag_id] = workflow\n"
    )


def publish(spec):
    """Publish only the managed bundle. Git-synced/read-only DAG folders use export."""
    from airflow.configuration import conf

    root = Path(conf.get("core", "dags_folder")).resolve()
    folder = root / "workbench_managed"
    if folder.is_symlink():
        raise ValueError("관리 DAG 폴더는 심볼릭 링크일 수 없습니다.")
    folder.mkdir(exist_ok=True)
    path = folder / (spec.id + ".py")
    if path.is_symlink():
        raise ValueError("관리 DAG 파일은 심볼릭 링크일 수 없습니다.")
    source = dag_source(spec)
    temporary = path.with_suffix(".pending")
    if temporary.is_symlink():
        raise ValueError("임시 DAG 파일은 심볼릭 링크일 수 없습니다.")
    temporary.write_text(source, encoding="utf-8")
    temporary.replace(path)
    return {
        "filename": path.name,
        "dag_ids": spec.dag_ids(),
        "fingerprint": spec.fingerprint(),
    }
