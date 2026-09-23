"""Inference and external training inputs; usable without Airflow installed."""

from typing import Annotated, Literal
from pydantic import Field, model_validator
from airflow_workbench.shared.contracts import Contract, ModelName, ShortName


class GenerationOptions(Contract):
    temperature: float = Field(default=0.2, ge=0, le=2)
    top_p: float = Field(default=0.9, gt=0, le=1)
    top_k: int = Field(default=40, ge=1, le=100)
    num_ctx: int = Field(default=4096, ge=512, le=16384)
    num_predict: int = Field(default=256, ge=16, le=2048)
    repeat_penalty: float = Field(default=1.1, ge=0.5, le=2)
    seed: int = Field(default=42, ge=0, le=2147483647)


class ChatExperiment(Contract):
    name: ShortName = "Generation comparison"
    models: list[ModelName] = Field(min_length=1, max_length=2)
    prompt: str = Field(min_length=1, max_length=12000)
    system: str = Field(
        default="한국어로 정확하고 간결하게 답하세요. 모르는 사실은 추측하지 마세요.",
        max_length=8000,
    )
    options: GenerationOptions = Field(default_factory=GenerationOptions)
    json_mode: bool = False

    @model_validator(mode="after")
    def distinct_models(self):
        if len(self.models) != len(set(self.models)):
            raise ValueError("비교할 모델은 서로 달라야 합니다.")
        return self


class EmbeddingExperiment(Contract):
    name: ShortName = "Embedding retrieval"
    models: list[ModelName] = Field(min_length=1, max_length=2)
    query: str = Field(min_length=1, max_length=2000)
    documents: list[Annotated[str, Field(min_length=1, max_length=4000)]] = Field(
        min_length=2, max_length=40
    )
    relevant_indices: list[int] = Field(default_factory=list, max_length=40)
    k: int = Field(default=3, ge=1, le=40)

    @model_validator(mode="after")
    def valid_labels(self):
        if len(set(self.models)) != len(self.models):
            raise ValueError("비교할 모델은 서로 달라야 합니다.")
        if len(set(self.relevant_indices)) != len(self.relevant_indices):
            raise ValueError("정답 문서 번호가 중복됩니다.")
        if any(i < 0 or i >= len(self.documents) for i in self.relevant_indices):
            raise ValueError("정답 문서 번호가 문서 범위를 벗어났습니다.")
        if self.k > len(self.documents):
            raise ValueError("K는 문서 수 이하여야 합니다.")
        return self


def storage_location(value, *, mounted=False):
    from urllib.parse import urlsplit

    if any(c in value for c in ("\\", "%", "\n", "\r", "\x00")):
        raise ValueError(
            "경로에는 역슬래시, 인코딩된 문자 또는 제어 문자를 사용할 수 없습니다."
        )
    parsed = urlsplit(value)
    if mounted:
        valid = (
            value.startswith("/")
            and not value.startswith("//")
            and not parsed.scheme
            and value != "/"
        )
    else:
        valid = (
            parsed.scheme in {"s3", "gs", "abfs", "abfss"}
            and bool(parsed.netloc)
            and bool(parsed.path.strip("/"))
        )
    if (
        not valid
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or any(p in {".", ".."} for p in parsed.path.split("/"))
    ):
        raise ValueError(
            "워커의 절대 경로 또는 자격 증명 없는 저장소 URI를 사용하세요."
        )
    return value.rstrip("/")


class TrainingStorage(Contract):
    profile_id: str = Field(default="", pattern=r"^[a-zA-Z0-9_-]{0,80}$")
    profile_version: int = Field(default=0, ge=0)
    mode: Literal["mounted", "object"] = "mounted"
    data_root: str = Field(min_length=1, max_length=800)
    model_root: str = Field(default="", max_length=800)
    artifact_root: str = Field(min_length=1, max_length=800)

    @model_validator(mode="after")
    def locations(self):
        mounted = self.mode == "mounted"
        self.data_root = storage_location(self.data_root, mounted=mounted)
        self.artifact_root = storage_location(self.artifact_root, mounted=mounted)
        if mounted:
            self.model_root = storage_location(self.model_root, mounted=True)
            for source in (self.data_root, self.model_root):
                if (
                    self.artifact_root == source
                    or self.artifact_root.startswith(source + "/")
                    or source.startswith(self.artifact_root + "/")
                ):
                    raise ValueError(
                        "결과 저장 경로는 원본 데이터·모델 경로와 분리하세요."
                    )
        elif self.model_root:
            raise ValueError(
                "객체 저장소 실행은 Hugging Face 모델 ID와 고정 revision으로 원본을 가져옵니다. 모델 루트는 비워두세요."
            )
        return self


class TrainingRecipe(Contract):
    storage: TrainingStorage | None = None
    environment_id: str = Field(default="", pattern=r"^(?:[a-z][a-z0-9_-]{0,47})?$")
    name: ShortName = "모델 파인튜닝"
    task: Literal["llm_sft", "embedding_contrastive"] = "llm_sft"
    method: Literal["lora", "qlora", "full"] = "qlora"
    base_model: ModelName
    revision: Annotated[str, Field(pattern=r"^[a-fA-F0-9]{40}$")]
    train_dataset: str = Field(min_length=1, max_length=1000)
    validation_dataset: str = Field(min_length=1, max_length=1000)
    train_sha256: str = Field(default="", pattern=r"^(?:[a-f0-9]{64})?$")
    validation_sha256: str = Field(default="", pattern=r"^(?:[a-f0-9]{64})?$")
    artifact_uri: str = Field(default="", max_length=1000)
    epochs: float = Field(default=1, gt=0, le=20)
    learning_rate: float = Field(default=0.0002, gt=0, le=0.01)
    batch_size: int = Field(default=1, ge=1, le=64)
    gradient_accumulation_steps: int = Field(default=16, ge=1, le=128)
    max_seq_length: int = Field(default=1024, ge=128, le=8192)
    lora_rank: int = Field(default=16, ge=4, le=128)
    lora_alpha: int = Field(default=32, ge=4, le=256)
    lora_dropout: float = Field(default=0.05, ge=0, le=0.5)
    warmup_ratio: float = Field(default=0.03, ge=0, le=0.3)
    weight_decay: float = Field(default=0.01, ge=0, le=1)
    seed: int = Field(default=42, ge=0, le=2147483647)
    output_name: Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")] = (
        "model-adapter-v1"
    )

    @model_validator(mode="after")
    def separate_data(self):
        import re
        from urllib.parse import urlsplit

        if self.storage:
            if self.storage.mode == "mounted":
                if (
                    any(
                        not re.fullmatch(r"[a-zA-Z0-9_-]+\.jsonl", v)
                        for v in (self.train_dataset, self.validation_dataset)
                    )
                    or self.artifact_uri
                ):
                    raise ValueError(
                        "마운트 저장소는 JSONL 파일명과 프로필의 결과 경로를 사용합니다."
                    )
            else:
                if (
                    any(
                        not v.startswith(self.storage.data_root + "/")
                        for v in (self.train_dataset, self.validation_dataset)
                    )
                    or self.artifact_uri != self.storage.artifact_root
                ):
                    raise ValueError(
                        "데이터·결과 URI는 선택한 저장소 프로필의 경로를 사용해야 합니다."
                    )
        for value in (self.train_dataset, self.validation_dataset, self.artifact_uri):
            if not value:
                continue
            if (
                re.fullmatch(r"[a-zA-Z0-9_-]+\.jsonl", value)
                and value != self.artifact_uri
            ):
                continue
            parsed = urlsplit(value)
            storage_location(value)
            if (
                parsed.scheme not in {"s3", "gs", "abfs", "abfss"}
                or not parsed.netloc
                or not parsed.path
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or ".." in parsed.path.split("/")
            ):
                raise ValueError(
                    "데이터는 실행기 JSONL 이름 또는 자격 증명 없는 s3/gs/abfs URI를 사용하세요."
                )
        if self.train_dataset == self.validation_dataset:
            raise ValueError("학습/검증 파일은 분리해야 합니다.")
        if self.task == "llm_sft" and self.method == "full":
            raise ValueError("생성 모델은 LoRA 또는 QLoRA를 사용하세요.")
        if self.task == "embedding_contrastive" and self.method != "full":
            raise ValueError("임베딩 학습 계약은 full contrastive 방식을 사용합니다.")
        return self


class Preset(Contract):
    name: ShortName
    kind: Literal["generation", "embedding", "training"]
    config: dict

    @model_validator(mode="after")
    def check_config(self):
        schema = {
            "generation": ChatExperiment,
            "embedding": EmbeddingExperiment,
            "training": TrainingRecipe,
        }[self.kind]
        self.config = schema.model_validate(self.config).model_dump()
        return self


class TrainingLaunch(Contract):
    recipe: TrainingRecipe
    request_id: Annotated[str, Field(pattern=r"^[a-f0-9-]{36}$")]
