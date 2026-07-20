from episcanner.analysis.richards import (
    comp_duration,
    equation,
    get_SIR_pars,
    objective,
)
from episcanner.schemas import FittedCurve, RichardsPars, SIRPars
from epiweeks import Week
import numpy as np


class TestEquation:
    def test_scalar(self):
        result = equation(100.0, 0.5, 0.3, np.array([0.0]), 5.0)
        assert result.shape == (1,)
        assert result[0] > 0

    def test_array(self):
        t = np.arange(10)
        result = equation(100.0, 0.5, 0.3, t, 5.0)
        assert result.shape == (10,)
        assert np.all(np.diff(result) >= 0)  # monotonically increasing


class TestObjective:
    def test_returns_mse(self):
        from lmfit import Parameters
        import pandas as pd

        df = pd.DataFrame(
            {
                "casos_cum": [10.0, 30.0, 70.0, 130.0, 200.0],
            }
        )
        params = Parameters()
        params.add("gamma", value=0.3)
        params.add("L1", value=250.0)
        params.add("tp1", value=3.0)
        params.add("b1", value=0.25)
        params.add("a1", value=0.45)

        result = objective(params, 0, df)
        assert result.shape == (5,)
        assert np.all(result >= 0)


class TestGetSIRPars:
    def test_from_model(self):
        rp = RichardsPars(gamma=0.3, L1=100.0, tp1=5.0, b1=0.3, a1=0.5)
        sir = get_SIR_pars(rp)
        assert isinstance(sir, SIRPars)
        assert sir.beta == 0.6
        assert sir.gamma == 0.3
        assert sir.R0 == 2.0
        assert sir.tc == 5.0

    def test_from_dict(self):
        rp = {"gamma": 0.3, "L1": 100.0, "tp1": 5.0, "b1": 0.15, "a1": 0.5}
        sir = get_SIR_pars(rp)
        assert isinstance(sir, SIRPars)
        assert sir.beta == 0.3
        assert sir.gamma == 0.15
        assert sir.R0 == 2.0


class TestCompDuration:
    def test_52_weeks(self):
        ew_list = [Week(2024, w) for w in range(1, 53)]
        fc = FittedCurve(
            ew=ew_list,
            casos_cum=list(range(52)),
            richards=[float(i) for i in range(52)],
        )
        ep = comp_duration(fc, tp1=5.0)
        assert ep.pw.startswith("2024")
        assert ep.dur > 0

    def test_53_weeks(self):
        ew_list = [Week(2020, w) for w in range(1, 54)]
        fc = FittedCurve(
            ew=ew_list,
            casos_cum=list(range(53)),
            richards=[float(i) for i in range(53)],
        )
        ep = comp_duration(fc, tp1=5.0)
        assert ep.pw.startswith("2020")
        assert ep.dur > 0

    def test_peak_after_end_returns_none(self):
        ew_list = [Week(2024, w) for w in range(1, 53)]
        fc = FittedCurve(
            ew=ew_list,
            casos_cum=list(range(52)),
            richards=[float(i) for i in range(52)],
        )
        ep = comp_duration(fc, tp1=60.0)
        assert ep.ini is None
        assert ep.end is None
