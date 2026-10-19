"""Shared strict, immutable contract configuration and scalar constraints."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class Contract(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", validate_default=True,
    )


NonBlankText = Annotated[str, Field(min_length=1, pattern=r"\S")]
Rate = Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]
Count = Annotated[int, Field(ge=0)]
