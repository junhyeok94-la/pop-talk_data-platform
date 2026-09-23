"""로컬 가중치의 SFT/contrastive 학습 adapter. Airflow 이미지에는 import하지 않는다."""

import hashlib
import json
import os
from pathlib import Path


def train(spec, folder):
    import torch
    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
        BitsAndBytesConfig,
        DataCollatorForSeq2Seq,
        Trainer,
        TrainingArguments,
        TrainerCallback,
        set_seed,
    )

    r = spec["payload"]["recipe"]
    set_seed(r["seed"])
    if r.get("storage") and r["storage"]["mode"] == "mounted":
        from airflow_workbench.model_lab.schemas import TrainingRecipe
        from airflow_workbench.model_lab.storage import model_directory, artifact_directory

        recipe = TrainingRecipe.model_validate(r)
        model_dir = model_directory(recipe)
        artifact_folder = artifact_directory(recipe, folder.name)
        artifact_folder.mkdir(parents=True, exist_ok=True)
    else:
        model_root = Path(
            os.environ.get("MODEL_WORKER_WEIGHTS_DIR", "/models")
        ).resolve()
        model_dir = (
            model_root / r["base_model"].replace("/", "--") / r["revision"]
        ).resolve()
        if not model_dir.is_relative_to(model_root):
            raise ValueError("model path boundary")
        artifact_folder = folder
    data_root = folder / "inputs"

    def rows(name):
        return [
            json.loads(line)
            for line in (data_root / name).read_text(encoding="utf-8-sig").splitlines()
            if line.strip()
        ]

    class Metrics(TrainerCallback):
        def on_log(self, args, state, control, logs=None, **kwargs):
            with (artifact_folder / "metrics.jsonl").open("a") as stream:
                stream.write(
                    json.dumps({"step": state.global_step, **(logs or {})}) + "\n"
                )

    output = artifact_folder / "model"
    if r["task"] == "llm_sft":
        from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training

        tokenizer = AutoTokenizer.from_pretrained(
            model_dir, local_files_only=True, trust_remote_code=False
        )
        if tokenizer.pad_token_id is None:
            tokenizer.pad_token = tokenizer.eos_token
        options = {
            "local_files_only": True,
            "trust_remote_code": False,
            "torch_dtype": torch.float16,
            "device_map": {"": 0},
            "low_cpu_mem_usage": True,
        }
        if r["method"] == "qlora":
            options["quantization_config"] = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_use_double_quant=True,
                bnb_4bit_compute_dtype=torch.float16,
            )
        model = AutoModelForCausalLM.from_pretrained(model_dir, **options)
        if r["method"] == "qlora":
            model = prepare_model_for_kbit_training(model)
        model = get_peft_model(
            model,
            LoraConfig(
                r=r["lora_rank"],
                lora_alpha=r["lora_alpha"],
                lora_dropout=r["lora_dropout"],
                target_modules="all-linear",
                task_type="CAUSAL_LM",
            ),
        )
        model.config.use_cache = False

        def tokenize(records):
            out = []
            for record in records:
                messages = record["messages"]
                if messages[-1]["role"] != "assistant":
                    raise ValueError("SFT 최종 메시지는 assistant여야 합니다.")
                prefix = tokenizer.apply_chat_template(
                    messages[:-1],
                    tokenize=True,
                    add_generation_prompt=True,
                    enable_thinking=False,
                )
                ids = tokenizer.apply_chat_template(
                    messages, tokenize=True, enable_thinking=False
                )[: r["max_seq_length"]]
                if ids[: len(prefix)] != prefix:
                    raise ValueError(
                        "chat template의 assistant prefix가 일치하지 않습니다. 모델별 token masking을 확인하세요."
                    )
                if len(ids) <= len(prefix):
                    raise ValueError(
                        "max_seq_length가 작아 학습할 assistant 토큰이 없습니다."
                    )
                out.append(
                    {
                        "input_ids": ids,
                        "attention_mask": [1] * len(ids),
                        "labels": [-100] * len(prefix) + ids[len(prefix) :],
                    }
                )
            return out

        args = TrainingArguments(
            output_dir=str(output),
            num_train_epochs=r["epochs"],
            learning_rate=r["learning_rate"],
            per_device_train_batch_size=r["batch_size"],
            per_device_eval_batch_size=1,
            gradient_accumulation_steps=r["gradient_accumulation_steps"],
            warmup_ratio=r["warmup_ratio"],
            weight_decay=r["weight_decay"],
            seed=r["seed"],
            gradient_checkpointing=True,
            fp16=True,
            eval_strategy="epoch",
            save_strategy="no",
            logging_steps=1,
            report_to=[],
            dataloader_num_workers=0,
        )
        trainer = Trainer(
            model=model,
            args=args,
            train_dataset=tokenize(rows(r["train_dataset"])),
            eval_dataset=tokenize(rows(r["validation_dataset"])),
            data_collator=DataCollatorForSeq2Seq(
                tokenizer, label_pad_token_id=-100, pad_to_multiple_of=8
            ),
            callbacks=[Metrics()],
        )
        trainer.train()
        metrics = trainer.evaluate()
        model.save_pretrained(output)
        tokenizer.save_pretrained(output)
    else:
        from sentence_transformers import (
            SentenceTransformer,
            SentenceTransformerTrainer,
            SentenceTransformerTrainingArguments,
            losses,
        )
        from datasets import Dataset

        model = SentenceTransformer(
            str(model_dir),
            device="cuda",
            local_files_only=True,
            trust_remote_code=False,
        )
        model.max_seq_length = r["max_seq_length"]

        def dataset(name):
            return Dataset.from_list(
                [
                    {key: row[key] for key in ("anchor", "positive", "negative")}
                    for row in rows(name)
                ]
            )

        args = SentenceTransformerTrainingArguments(
            output_dir=str(output),
            num_train_epochs=r["epochs"],
            learning_rate=r["learning_rate"],
            per_device_train_batch_size=r["batch_size"],
            per_device_eval_batch_size=1,
            gradient_accumulation_steps=r["gradient_accumulation_steps"],
            warmup_ratio=r["warmup_ratio"],
            weight_decay=r["weight_decay"],
            seed=r["seed"],
            fp16=True,
            eval_strategy="epoch",
            save_strategy="no",
            logging_steps=1,
            report_to=[],
        )
        trainer = SentenceTransformerTrainer(
            model=model,
            args=args,
            train_dataset=dataset(r["train_dataset"]),
            eval_dataset=dataset(r["validation_dataset"]),
            loss=losses.MultipleNegativesRankingLoss(model),
            callbacks=[Metrics()],
        )
        trainer.train()
        metrics = trainer.evaluate()
        model.save_pretrained(str(output))
    receipt = {
        "model": r["base_model"],
        "revision": r["revision"],
        "recipe_sha256": spec["payload"]["recipe_sha256"],
        "datasets": spec["payload"]["datasets"],
        "training_performed": True,
        "metrics": metrics,
        "max_gpu_allocated_mib": torch.cuda.max_memory_allocated() // 1048576,
        "artifact_uri": artifact_folder.as_uri(),
        "storage": r.get("storage"),
    }
    (artifact_folder / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")
    return receipt
