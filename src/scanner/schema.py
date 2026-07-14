from typing import Literal

from pydantic import BaseModel, Field


class SirParams(BaseModel):
    disease: str
    cid10: Literal["A90", "A92.8", "A92.0"] = Field(validation_alias="CID10")
    geocode: int
    muni_name: str
    year: int
    ep_ini: str | None = None
    ep_pw: str
    ep_end: str | None = None
    ep_dur: int | None = None
    peak_week: float
    beta: float
    gamma: float
    R0: float = Field(validation_alias="R0")
    total_cases: float
    alpha: float
    sum_res: float
    t_ini: int | None = None
    t_end: int | None = None
