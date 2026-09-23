"""Small provider-neutral input primitives shared by both features."""

from typing import Annotated
from pydantic import BaseModel, ConfigDict, Field


class Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid", allow_inf_nan=False, validate_default=True
    )


ModelName = Annotated[str, Field(min_length=1, max_length=160, pattern=r"^[\w./:@-]+$")]
ShortName = Annotated[str, Field(min_length=1, max_length=100)]
