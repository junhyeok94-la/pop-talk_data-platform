"""Legacy dashboard inputs."""

from typing import Annotated, Literal
from pydantic import Field, model_validator
from airflow_workbench.shared.contracts import Contract, ShortName


class Panel(Contract):
    id: Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")]
    title: ShortName
    source: Literal[
        "run_count",
        "success_rate",
        "model_count",
        "experiment_count",
        "dag_runs",
        "run_states",
        "run_duration",
        "experiments",
    ]
    width: Literal[4, 6, 8, 12] = 6


class Dashboard(Contract):
    title: ShortName = "Operations overview"
    version: int = Field(default=0, ge=0)
    hours: Literal[6, 24, 168, 720] = 24
    dag_filter: str = Field(default="", max_length=100)
    refresh_seconds: Literal[0, 30, 60, 300] = 60
    panels: list[Panel] = Field(min_length=1, max_length=24)

    @model_validator(mode="after")
    def unique_ids(self):
        if len({p.id for p in self.panels}) != len(self.panels):
            raise ValueError("패널 ID는 중복될 수 없습니다.")
        return self
