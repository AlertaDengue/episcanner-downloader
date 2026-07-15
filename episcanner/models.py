from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from epiweeks import Week
import lmfit as lm
from lmfit import Parameters
import numpy as np
import numpy.typing as npt
import pandas as pd

from .analysis.richards import comp_duration, equation, get_SIR_pars, objective
from .schemas import (
    AlertaRow,
    AlertRow,
    EpDuration,
    FittedCurve,
    RichardsPars,
    SIRPars,
)

THR_PROB = 0.9
N_WEEKS = 3
CUM_CASES = 50


class AnalysisModel(ABC):
    @staticmethod
    @abstractmethod
    def objective(
        params: Parameters,
        aux: int,
        df: pd.DataFrame,
    ) -> npt.NDArray[np.float64]:
        ...

    @staticmethod
    @abstractmethod
    def fit(
        data: Sequence[AlertRow],
        verbose: bool = False,
    ) -> AnalysisModel:
        ...


class Richards(AnalysisModel):
    def __init__(
        self, L: float, a: float, b: float, tp1: float, gamma: float
    ) -> None:
        self.L = L
        self.a = a
        self.b = b
        self.tp1 = tp1
        self.gamma = gamma

    def evaluate(self, t: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return equation(  # type: ignore[no-any-return]
            self.L, self.a, self.b, t, self.tp1
        )

    def to_curve(self, data: Sequence[AlertRow | AlertaRow]) -> FittedCurve:
        df = pd.DataFrame(
            [
                {"data_iniSE": row.ew.startdate(), "casos_est": row.casos_est}
                for row in data
            ]
        )
        df["casos_cum"] = df.casos_est.cumsum()
        t_range = np.arange(df.shape[0])
        richfun = self.evaluate(t_range)
        return FittedCurve(
            ew=[Week.fromdate(d) for d in df.data_iniSE],
            casos_cum=df.casos_cum.tolist(),
            richards=richfun.tolist(),
        )

    @staticmethod
    def objective(
        params: Parameters,
        aux: int,
        df: pd.DataFrame,
    ) -> npt.NDArray[np.float64]:
        return objective(params, aux, df)  # type: ignore[no-any-return]

    @staticmethod
    def fit(
        data: Sequence[AlertRow | AlertaRow],
        verbose: bool = False,
    ) -> Richards:
        df = pd.DataFrame(
            [
                {"data_iniSE": row.ew.startdate(), "casos_est": row.casos_est}
                for row in data
            ]
        )
        df["casos_cum"] = df.casos_est.cumsum()
        sum_cases = df.casos_est.sum()
        params = Parameters()
        params.add("gamma", min=0.3, max=0.33)
        params.add("L1", min=1.0, max=1.2 * sum_cases)
        params.add("tp1", min=5, max=35)
        params.add("b1", min=1e-6, max=1)
        params.add("a1", expr="b1/(gamma + b1)", min=0.001, max=1)

        out = lm.minimize(
            Richards.objective,
            params,
            args=(0, df),
            method="diferential_evolution",
        )

        if verbose:
            if out.success:
                print(f"found match after {out.nfev} tries")
            else:
                print("No match found")

        pars = out.params.valuesdict()
        return Richards(
            L=pars["L1"],
            a=pars["a1"],
            b=pars["b1"],
            tp1=pars["tp1"],
            gamma=pars["gamma"],
        )

    def get_SIR_pars(self) -> SIRPars:
        return get_SIR_pars(
            RichardsPars(
                gamma=self.gamma,
                L1=self.L,
                tp1=self.tp1,
                b1=self.b,
                a1=self.a,
            )
        )

    def comp_duration(self, curve: FittedCurve) -> EpDuration:
        return comp_duration(curve, self.tp1)

    @staticmethod
    def scan(
        data: Sequence[AlertaRow],
        year: int,
    ) -> tuple[dict[int, Richards], dict[int, FittedCurve]]:
        models: dict[int, Richards] = {}
        curves: dict[int, FittedCurve] = {}
        for geocode in {r.geocode for r in data}:
            result = Richards._scan_geocode(data, geocode, year)
            if result is not None:
                model, curve = result
                models[geocode] = model
                curves[geocode] = curve
        return models, curves

    @staticmethod
    def _scan_geocode(
        data: Sequence[AlertaRow],
        geocode: int,
        year: int,
    ) -> tuple[Richards, FittedCurve] | None:
        city_data = sorted(
            (r for r in data if r.geocode == geocode),
            key=lambda r: r.ew,
        )

        window = [
            r
            for r in city_data
            if (
                (r.ew.year == year - 1 and r.ew.week >= 45)
                or (r.ew.year == year and r.ew.week <= 35)
            )
        ]

        high_rt1 = sum(1 for r in window if r.p_rt1 > THR_PROB)
        total_cases = sum(r.casos_est for r in window)

        if high_rt1 <= N_WEEKS or total_cases <= CUM_CASES:
            return None

        fit_data = [r for r in city_data if r.ew.year in (year - 1, year)]

        model = Richards.fit(fit_data)
        curve = model.to_curve(fit_data)
        return model, curve
