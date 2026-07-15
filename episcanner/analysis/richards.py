from __future__ import annotations

from datetime import timedelta

from epiweeks import Week
from lmfit import Parameters
import numpy as np
import numpy.typing as npt
import pandas as pd

from ..schemas import EpDuration, FittedCurve, RichardsPars, SIRPars


@np.vectorize
def equation(  # noqa: E501
    L: float,
    a: float,
    b: float,
    t: npt.NDArray[np.float64],
    tj: float,
) -> npt.NDArray[np.float64]:
    return L - L * (  # type: ignore[no-any-return]
        1 + a * np.exp(b * (t - tj))
    ) ** (-1 / a)


def objective(
    params: Parameters,
    aux: int,
    df: pd.DataFrame,
) -> npt.NDArray[np.float64]:
    window = df.shape[0]
    pars = params.valuesdict()
    L = pars["L1"]
    tp = pars["tp1"]
    a = pars["a1"]
    b = pars["b1"]

    t_range = np.arange(window)
    richfun = equation(L, a, b, t_range, tp)
    serie = df.casos_cum.values

    mse = (serie - richfun) ** 2 / window
    return mse  # type: ignore[no-any-return]


def get_SIR_pars(rp: RichardsPars | dict[str, float]) -> SIRPars:
    if isinstance(rp, dict):
        rp = RichardsPars.model_validate(rp)
    assert isinstance(rp, RichardsPars)
    a = rp.a1
    b = rp.b1
    tc = rp.tp1
    return SIRPars(
        beta=b / a,
        gamma=(b / a) - b,
        R0=(b / a) / ((b / a) - b),
        tc=tc,
    )


def comp_duration(curve: FittedCurve, tp1: float) -> EpDuration:
    richards_arr = np.array(curve.richards)
    df_aux = pd.DataFrame()
    df_aux["SE"] = [w.cdcformat() for w in curve.ew[:52]]

    pw_date = Week.fromstring(str(df_aux.SE[0])).startdate() + timedelta(
        days=7
    ) * int(round(tp1, 0))
    pw = Week.fromdate(pw_date).cdcformat()

    df_aux["diff_richards"] = np.concatenate(
        ([0], np.diff(richards_arr)), axis=0
    )
    max_c = df_aux["diff_richards"].max()
    df_aux = df_aux.loc[df_aux.diff_richards >= (0.05) * max_c].sort_index()

    ini_str = str(df_aux["SE"].values[0])
    end_str = str(df_aux["SE"].values[-1])

    ini_aux = Week.fromstring(ini_str).startdate()
    end_aux = Week.fromstring(end_str).startdate()
    dur = int((end_aux - ini_aux).days / 7)

    ew_startdates = np.array([w.startdate() for w in curve.ew])
    target_ini = Week.fromstring(ini_str).startdate()
    target_end = Week.fromstring(end_str).startdate()
    t_ini = np.where(ew_startdates == target_ini)[0][0]
    t_end = np.where(ew_startdates == target_end)[0][0]

    if (
        Week.fromstring(str(pw)).startdate()
        - Week.fromstring(end_str).startdate()
    ).days >= 0:
        ini = None
        end = None
        t_ini = None
        t_end = None
    else:
        ini = ini_str
        end = end_str

    return EpDuration(
        ini=ini,
        pw=pw,
        end=end,
        dur=dur,
        t_ini=t_ini,
        t_end=t_end,
    )
