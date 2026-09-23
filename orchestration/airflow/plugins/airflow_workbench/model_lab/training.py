"""실행기에서 수행하는 JSONL 검증. DAG/실행 환경 설정은 제어 API가 검증한다."""

import hashlib
import json
import os
from pathlib import Path


def training_root():
    return Path(
        os.environ.get(
            "AIRFLOW_WORKBENCH_TRAINING_DATA_DIR",
            "/datasets/training",
        )
    )


def dataset_catalog():
    root = training_root()
    if not root.is_dir():
        return []
    return [
        {"name": p.name, "bytes": p.stat().st_size}
        for p in sorted(root.glob("*.jsonl"))
        if p.is_file() and not p.is_symlink()
    ]


def validate_dataset(name, split, task, *, root=None):
    root = (root or training_root()).resolve()
    path = (root / name).resolve()
    if path.parent != root or not path.is_file():
        raise ValueError(f"데이터 파일이 없습니다: {name}")
    if path.stat().st_size > 20 * 1024 * 1024:
        raise ValueError(
            "화면 검증은 파일당 20MB까지 지원합니다. 큰 데이터는 실행기에서 별도 검증하세요."
        )
    data = path.read_bytes()
    groups, examples = set(), set()
    count = 0
    for number, line in enumerate(data.decode("utf-8-sig").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if (
            not isinstance(row, dict)
            or row.get("split") != split
            or not isinstance(row.get("family_id"), str)
            or not row["family_id"].strip()
        ):
            raise ValueError(
                f"{name}:{number} split={split}, family_id가 필요합니다. 평가/holdout 자료는 학습에 사용할 수 없습니다."
            )
        if task == "llm_sft":
            messages = row.get("messages")
            if (
                not isinstance(messages, list)
                or len(messages) < 2
                or any(
                    not isinstance(m, dict)
                    or m.get("role") not in {"system", "user", "assistant"}
                    or not isinstance(m.get("content"), str)
                    or not m["content"].strip()
                    for m in messages
                )
            ):
                raise ValueError(f"{name}:{number} 유효한 messages가 필요합니다.")
            if not any(m["role"] == "user" for m in messages) or not any(
                m["role"] == "assistant" for m in messages
            ):
                raise ValueError(f"{name}:{number} user/assistant 메시지가 필요합니다.")
            content = messages
        else:
            if any(
                not isinstance(row.get(key), str) or not row[key].strip()
                for key in ("anchor", "positive", "negative")
            ):
                raise ValueError(
                    f"{name}:{number} anchor/positive/negative가 필요합니다."
                )
            content = [row[key] for key in ("anchor", "positive", "negative")]
        groups.add(row["family_id"].strip())
        examples.add(
            hashlib.sha256(
                json.dumps(content, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()
        )
        count += 1
    if not count:
        raise ValueError(f"빈 데이터 파일입니다: {name}")
    return (
        {"name": name, "rows": count, "sha256": hashlib.sha256(data).hexdigest()},
        groups,
        examples,
    )


def inspect_recipe(recipe):
    blockers, datasets = [], []
    try:
        root = None
        if recipe.storage:
            from airflow_workbench.model_lab.storage import resolve_locations

            root = resolve_locations(recipe.storage)["data_root"]
        train, train_groups, train_examples = validate_dataset(
            recipe.train_dataset, "train", recipe.task, root=root
        )
        validation, validation_groups, validation_examples = validate_dataset(
            recipe.validation_dataset, "validation", recipe.task, root=root
        )
        datasets = [train, validation]
        if recipe.train_sha256 and recipe.train_sha256 != train["sha256"]:
            blockers.append("학습 데이터 SHA256이 입력과 다릅니다.")
        if (
            recipe.validation_sha256
            and recipe.validation_sha256 != validation["sha256"]
        ):
            blockers.append("검증 데이터 SHA256이 입력과 다릅니다.")
        if train_groups & validation_groups:
            blockers.append("학습/검증 데이터의 family_id가 겹칩니다.")
        if train_examples & validation_examples:
            blockers.append("학습/검증 데이터에 동일한 내용이 포함되어 있습니다.")
    except (ValueError, OSError, UnicodeError) as exc:
        blockers.append(
            str(exc)
            if isinstance(exc, ValueError)
            else "학습 데이터 파일을 읽을 수 없습니다."
        )
    spec = recipe.model_dump()
    digest = hashlib.sha256(
        json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "ready": not blockers,
        "blockers": blockers,
        "datasets": datasets,
        "recipe_sha256": digest,
        "effective_batch_size": recipe.batch_size * recipe.gradient_accumulation_steps,
        "contract_version": 2,
        "recipe": spec,
    }
