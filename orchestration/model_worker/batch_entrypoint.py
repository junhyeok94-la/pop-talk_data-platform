"""Reference Kubernetes training image entrypoint. No Airflow/metadata DB access.

Object-storage credentials come from workload identity. A completion manifest is
published last, and XCom contains only the artifact URI and small summary.
"""

import hashlib
import json
import os
from pathlib import Path


def stage_dataset(uri, destination, expected_sha256):
    import fsspec

    digest = hashlib.sha256()
    size = 0
    with fsspec.open(uri, "rb") as source, destination.open("wb") as target:
        while chunk := source.read(1024 * 1024):
            size += len(chunk)
            if size > 20 * 1024 * 1024:
                raise ValueError(
                    "Reference trainer supports JSONL files up to 20 MiB; use a streaming trainer image for larger datasets."
                )
            digest.update(chunk)
            target.write(chunk)
    if digest.hexdigest() != expected_sha256:
        raise ValueError("Dataset content does not match the submitted SHA256")


def main(*, output_path="/airflow/xcom/return.json"):
    import fsspec
    from airflow_workbench.model_lab.portable_training import validate_remote_recipe

    request = json.loads(os.environ["WORKBENCH_REQUEST"])
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if request["kind"] == "diagnostic":
        from cuda_probe import probe

        output.write_text(
            json.dumps(
                {
                    "kind": "diagnostic",
                    "training_performed": False,
                    "cuda_probe": probe(),
                }
            )
        )
        return
    report = request["workbench"]
    recipe = validate_remote_recipe(report["recipe"], request["kind"])
    identity = "|".join([recipe.environment_id, request["kind"], request["run_id"]])
    key = hashlib.sha256(identity.encode()).hexdigest()[:24]
    artifact = recipe.artifact_uri.rstrip("/") + "/" + key
    filesystem, remote = fsspec.core.url_to_fs(artifact)
    marker = remote + "/_SUCCESS.json"
    if filesystem.exists(marker):
        with filesystem.open(marker, "r") as stream:
            result = json.load(stream)
        if result.get("recipe_sha256") != report["recipe_sha256"]:
            raise ValueError("Artifact identity belongs to a different recipe")
    else:
        folder = Path(os.environ.get("WORKBENCH_SCRATCH_DIR", "/tmp/workbench")) / key
        inputs = folder / "inputs"
        inputs.mkdir(parents=True, exist_ok=True)
        stage_dataset(recipe.train_dataset, inputs / "train.jsonl", recipe.train_sha256)
        stage_dataset(
            recipe.validation_dataset,
            inputs / "validation.jsonl",
            recipe.validation_sha256,
        )
        os.environ["AIRFLOW_WORKBENCH_TRAINING_DATA_DIR"] = str(inputs)
        from airflow_workbench.model_lab.training import validate_dataset

        train, train_groups, train_examples = validate_dataset(
            "train.jsonl", "train", recipe.task
        )
        validation, validation_groups, validation_examples = validate_dataset(
            "validation.jsonl", "validation", recipe.task
        )
        if train_groups & validation_groups or train_examples & validation_examples:
            raise ValueError("Training and validation datasets overlap")
        from huggingface_hub import snapshot_download

        model_root = folder / "weights"
        model_dir = model_root / recipe.base_model.replace("/", "--") / recipe.revision
        snapshot_download(
            repo_id=recipe.base_model,
            revision=recipe.revision,
            local_dir=model_dir,
            allow_patterns=[
                "*.json",
                "*.safetensors",
                "*.model",
                "*.tiktoken",
                "*.txt",
            ],
        )
        os.environ["MODEL_WORKER_WEIGHTS_DIR"] = str(model_root)
        from train import train as train_model

        local_recipe = {
            **recipe.model_dump(),
            "storage": None,
            "train_dataset": "train.jsonl",
            "validation_dataset": "validation.jsonl",
        }
        datasets = [
            {**train, "uri": recipe.train_dataset},
            {**validation, "uri": recipe.validation_dataset},
        ]
        result = train_model(
            {"payload": {**report, "datasets": datasets, "recipe": local_recipe}},
            folder,
        )
        result.update(
            artifact_uri=artifact,
            recipe_sha256=report["recipe_sha256"],
            training_performed=True,
        )
        for path in [
            *(folder / "model").rglob("*"),
            folder / "metrics.jsonl",
            folder / "receipt.json",
        ]:
            if path.is_file():
                filesystem.put_file(
                    str(path), remote + "/" + path.relative_to(folder).as_posix()
                )
        with filesystem.open(marker, "w") as stream:
            json.dump(result, stream)
    output.write_text(
        json.dumps(
            {
                k: result.get(k)
                for k in [
                    "training_performed",
                    "artifact_uri",
                    "recipe_sha256",
                    "metrics",
                ]
            }
        )
    )


if __name__ == "__main__":
    main()
