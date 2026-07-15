from datetime import datetime
from typing import Sequence, TypeAlias

from epiweeks import Week
import pandas as pd
from pydantic import BaseModel, ConfigDict


class AlertaRow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ew: Week
    casos_est: float
    geocode: int
    p_rt1: float


class AlertRow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ew: Week
    casos_est: float


class FittedCurve(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ew: list[Week]
    casos_cum: list[float]
    richards: list[float]


class RichardsPars(BaseModel):
    gamma: float
    L1: float
    tp1: float
    b1: float
    a1: float


class SIRPars(BaseModel):
    beta: float
    gamma: float
    R0: float
    tc: float


class EpDuration(BaseModel):
    ini: str | None
    pw: str
    end: str | None
    dur: int
    t_ini: int | None
    t_end: int | None


class SirParams(BaseModel):
    geocode: int
    year: int
    ep_ini: str | None = None
    ep_pw: str
    ep_end: str | None = None
    ep_dur: int | None = None
    peak_week: float
    beta: float
    gamma: float
    R0: float
    total_cases: float
    alpha: float
    sum_res: float
    t_ini: int | None = None
    t_end: int | None = None


AlertaData: TypeAlias = (
    pd.DataFrame | dict | Sequence[dict] | AlertaRow | Sequence[AlertaRow]
)


def parse_alerta(data: AlertaData) -> list[AlertaRow]:

    if isinstance(data, AlertaRow):
        return [data]

    if isinstance(data, list) and all(isinstance(r, AlertaRow) for r in data):
        return data

    if isinstance(data, pd.DataFrame):
        rows: list[dict] = data.to_dict(orient="records")
        return [_dict_to_alerta(r) for r in rows]

    if isinstance(data, dict):
        return [_dict_to_alerta(data)]

    if isinstance(data, list):
        return [_dict_to_alerta(r) for r in data]

    raise TypeError(
        f"Expected DataFrame, dict, list[dict], or AlertaRow, got {type(data)}"
    )


def _se_to_week(se: int) -> Week:
    return Week(se // 100, se % 100)


def _dict_to_alerta(row: dict) -> AlertaRow:
    ew = row.get("ew")
    if ew is None:
        if "SE" in row:
            ew = _se_to_week(int(row["SE"]))
        elif "data_iniSE" in row:
            d = row["data_iniSE"]
            if isinstance(d, datetime):
                ew = Week.fromdate(d)
            else:
                ew = Week.fromdate(pd.to_datetime(d))
    if not isinstance(ew, Week):
        raise ValueError(f"Cannot derive Week from row: {row}")

    return AlertaRow(
        ew=ew,
        casos_est=float(row["casos_est"]),
        geocode=int(row.get("geocode", row.get("municipio_geocodigo", 0))),
        p_rt1=float(row["p_rt1"]),
    )
