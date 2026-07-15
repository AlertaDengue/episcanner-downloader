from episcanner.models import AnalysisModel, Richards
from episcanner.schemas import AlertRow, EpDuration, FittedCurve, SIRPars
from epiweeks import Week
import numpy as np


def _make_data():
    return [
        AlertRow(ew=Week(2024, w), casos_est=c)
        for w, c in enumerate(
            [10, 25, 60, 120, 200, 280, 340, 370, 390, 400, 405, 408],
            start=1,
        )
    ]


class TestAnalysisModel:
    def test_cannot_instantiate_abc(self):
        with __import__("pytest").raises(TypeError):
            AnalysisModel()


class TestRichardsFitAndEvaluate:
    def test_fit_returns_richards(self):
        data = _make_data()
        model = Richards.fit(data)
        assert isinstance(model, Richards)
        assert model.L > 0
        assert model.a > 0
        assert model.b > 0
        assert model.tp1 > 0
        assert 0.3 <= model.gamma <= 0.33

    def test_evaluate_same_length_as_input(self):
        data = _make_data()
        model = Richards.fit(data)
        y = model.evaluate(np.arange(len(data)))
        assert len(y) == len(data)

    def test_evaluate_monotonic(self):
        data = _make_data()
        model = Richards.fit(data)
        y = model.evaluate(np.arange(len(data)))
        assert np.all(np.diff(y) >= 0)

    def test_to_curve(self):
        data = _make_data()
        model = Richards.fit(data)
        curve = model.to_curve(data)
        assert isinstance(curve, FittedCurve)
        assert len(curve.ew) == len(data)
        assert len(curve.casos_cum) == len(data)
        assert len(curve.richards) == len(data)
        assert curve.casos_cum[-1] >= curve.casos_cum[0]

    def test_get_sir_pars(self):
        data = _make_data()
        model = Richards.fit(data)
        sir = model.get_SIR_pars()
        assert isinstance(sir, SIRPars)
        assert sir.R0 > 0
        assert sir.beta > 0
        assert sir.gamma > 0

    def test_comp_duration(self):
        data = _make_data()
        model = Richards.fit(data)
        curve = model.to_curve(data)
        ep = model.comp_duration(curve)
        assert isinstance(ep, EpDuration)
        assert ep.dur > 0


class TestRichardsInstatiation:
    def test_manual_params(self):
        model = Richards(L=3354.0, a=0.62, b=0.49, tp1=8.0, gamma=0.3)
        y = model.evaluate(np.array([0, 5, 10]))
        assert len(y) == 3
        assert y[0] > 0
        assert y[0] < y[-1]

    def test_is_subclass_of_analysis_model(self):
        assert issubclass(Richards, AnalysisModel)
