"""Portable Model Lab inputs. Credentials and executable code are never UI inputs."""

import hashlib
import json
from typing import Annotated, Literal

from pydantic import Field, model_validator, field_validator
from airflow_workbench.model_lab.schemas import (
    Contract,
    ModelName,
    TrainingRecipe,
    TrainingStorage,
)

Key = Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")]


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


class Project(Contract):
    id: Key
    name: str = Field(min_length=1, max_length=100)
    description: str = Field(default="", max_length=2000)
    members: dict[str, Literal["viewer", "developer", "manager"]] = Field(
        default_factory=dict, max_length=100
    )
    version: int = Field(default=0, ge=0)


class Endpoint(Contract):
    id: Key
    name: str = Field(min_length=1, max_length=100)
    provider: Literal["ollama", "openai", "gemini", "evaluation_api"] = "ollama"
    connection_id: str = Field(default="", pattern=r"^[a-zA-Z0-9_-]{0,128}$")
    capabilities: list[Literal["generation", "embedding", "agent"]] = Field(
        default_factory=lambda: ["generation", "embedding"], min_length=1, max_length=3
    )
    models: list[ModelName] = Field(default_factory=list, max_length=100)
    pool: str = Field(default="model_lab_gpu", pattern=r"^[a-zA-Z0-9_-]{1,80}$")
    capacity: int = Field(default=1, ge=1, le=16)
    reservation_environment: str = Field(default="", pattern=r"^[a-zA-Z0-9_-]{0,80}$")
    timeout_seconds: int = Field(default=600, ge=10, le=1200)
    version: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def compatible(self):
        if not self.connection_id and not (
            self.id == "default-ollama" and self.provider == "ollama"
        ):
            raise ValueError("등록한 Airflow Connection ID가 필요합니다.")
        supported = {
            "ollama": {"generation", "embedding"},
            "openai": {"generation", "embedding"},
            "gemini": {"generation"},
            "evaluation_api": {"agent"},
        }[self.provider]
        if not set(self.capabilities) <= supported:
            raise ValueError("제공자가 지원하지 않는 평가 종류입니다.")
        return self

    def dag_id(self):
        return "model_lab_eval_" + self.id.replace("-", "_")

    def fingerprint(self):
        return digest(self.model_dump())[:20]


class Document(Contract):
    id: Key
    text: str = Field(min_length=1, max_length=8000)


class Case(Contract):
    id: Key
    input: str = Field(min_length=1, max_length=12000)
    category: str = Field(default="general", max_length=100)
    review_status: Literal["draft", "reviewed"] = "draft"
    expected_json: dict | None = None
    reference: dict = Field(default_factory=dict)
    required_text: list[str] = Field(default_factory=list, max_length=20)
    relevance: dict[str, int] | None = None

    @model_validator(mode="after")
    def labels(self):
        if self.relevance is not None and any(
            v not in (0, 1, 2, 3) for v in self.relevance.values()
        ):
            raise ValueError("관련도 라벨은 0~3입니다. 미라벨은 null로 표시하세요.")
        return self


class Dataset(Contract):
    name: str = Field(min_length=1, max_length=100)
    kind: Literal["generation", "embedding", "agent"]
    description: str = Field(default="", max_length=2000)
    split: Literal["development", "validation", "holdout"] = "development"
    cases: list[Case] = Field(min_length=1, max_length=100)
    corpus: list[Document] = Field(default_factory=list, max_length=500)
    corpus_version: str = Field(default="", max_length=200)

    @model_validator(mode="after")
    def valid_dataset(self):
        if len({c.id for c in self.cases}) != len(self.cases):
            raise ValueError("사례 ID가 중복됩니다.")
        ids = {d.id for d in self.corpus}
        if len(ids) != len(self.corpus):
            raise ValueError("문서 ID가 중복됩니다.")
        if self.kind == "embedding" and not self.corpus:
            raise ValueError("임베딩 평가에는 고정된 코퍼스가 필요합니다.")
        if any(
            c.relevance is not None and not set(c.relevance) <= ids for c in self.cases
        ):
            raise ValueError("관련도 라벨이 코퍼스에 없는 문서를 참조합니다.")
        return self


class Experiment(Contract):
    name: str = Field(min_length=1, max_length=100)
    hypothesis: str = Field(default="", max_length=4000)
    default_system: str = Field(default="", max_length=8000)
    baseline_run_id: str = Field(default="", pattern=r"^[a-f0-9]{0,32}$")


class RequestContract(Contract):
    request_id: str = Field(pattern=r"^[a-f0-9-]{36}$")

    @field_validator("request_id")
    @classmethod
    def uuid_request(cls, value):
        import uuid

        if str(uuid.UUID(value)) != value:
            raise ValueError("정규 UUID 요청 ID를 사용하세요.")
        return value


class Evaluation(RequestContract):
    request_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    experiment_id: Key
    dataset_id: Key
    endpoint_id: Key
    model: ModelName
    system: str = Field(default="", max_length=8000)
    temperature: float = Field(default=0.2, ge=0, le=2)
    max_tokens: int = Field(default=256, ge=16, le=2048)
    json_mode: bool = False
    options: dict[str, float] = Field(default_factory=dict, max_length=10)
    k: int = Field(default=5, ge=1, le=100)
    model_version_id: str = Field(default="", pattern=r"^[a-f0-9]{0,32}$")


class LabTraining(RequestContract):
    request_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    experiment_id: Key
    recipe: TrainingRecipe


class StorageProfile(Contract):
    id: Key
    name: str = Field(min_length=1, max_length=100)
    environment_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,47}$")
    description: str = Field(default="", max_length=2000)
    locations: TrainingStorage
    default_model: str = Field(default="", max_length=160, pattern=r"^[\w./:@-]*$")
    default_revision: str = Field(default="", pattern=r"^(?:[a-fA-F0-9]{40})?$")
    version: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def paired_model(self):
        if self.default_revision and not self.default_model:
            raise ValueError("기본 revision과 함께 기본 모델 ID를 입력하세요.")
        return self


class RegisterModel(Contract):
    name: str = Field(min_length=1, max_length=100)
    evaluation_run_id: Key
    training_run_id: str = Field(default="", pattern=r"^[a-f0-9]{0,32}$")
    artifact_uri: str = Field(default="", max_length=1000)
    notes: str = Field(default="", max_length=4000)


class Review(Contract):
    decision: Literal["approved", "rejected"]
    reason: str = Field(min_length=3, max_length=2000)


class CaseReview(Contract):
    score: Literal[0, 0.5, 1]
    reason: str = Field(min_length=3, max_length=2000)


class Release(Contract):
    model_version_id: Key
    target: str = Field(min_length=1, max_length=100)
    notes: str = Field(default="", max_length=2000)
    search_config: dict = Field(default_factory=dict)
