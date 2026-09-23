"""Stable DAG naming contract consumed by orchestration and monitoring."""


def dag_ids(prefix, workloads):
    suffix = {
        "diagnostic": "diagnostic",
        "llm_sft": "llm_train",
        "embedding_contrastive": "embedding_train",
    }
    return {kind: prefix + "_" + suffix[kind] for kind in workloads if kind in suffix}
