"""Compatibility exports. New code imports feature-specific schemas."""

from airflow_workbench.shared.contracts import Contract, ModelName, ShortName
from airflow_workbench.dashboard.schemas import Dashboard, Panel
from airflow_workbench.model_lab.schemas import (
    GenerationOptions,
    ChatExperiment,
    EmbeddingExperiment,
    storage_location,
    TrainingStorage,
    TrainingRecipe,
    Preset,
    TrainingLaunch,
)
