from datetime import datetime

from episcanner.schemas import (
    AlertaRow,
    AlertRow,
    EpDuration,
    FittedCurve,
    RichardsPars,
    SirParams,
    SIRPars,
    parse_alerta,
)
from epiweeks import Week


class TestAlertaRow:
    def test_create(self):
        r = AlertaRow(
            ew=Week(2024, 1), casos_est=10.5, geocode=3550308, p_rt1=0.95
        )
        assert r.ew == Week(2024, 1)
        assert r.casos_est == 10.5
        assert r.geocode == 3550308
        assert r.p_rt1 == 0.95

    def test_dict(self):
        r = AlertaRow(
            ew=Week(2024, 1), casos_est=10.5, geocode=3550308, p_rt1=0.95
        )
        d = r.model_dump()
        assert d["ew"] == Week(2024, 1)
        assert d["casos_est"] == 10.5
        assert d["geocode"] == 3550308
        assert d["p_rt1"] == 0.95


class TestAlertRow:
    def test_create(self):
        r = AlertRow(ew=Week(2024, 1), casos_est=10.5)
        assert r.ew == Week(2024, 1)
        assert r.casos_est == 10.5


class TestRichardsPars:
    def test_create(self):
        p = RichardsPars(gamma=0.3, L1=100.0, tp1=5.0, b1=0.2, a1=0.4)
        assert p.gamma == 0.3
        assert p.L1 == 100.0
        assert p.tp1 == 5.0

    def test_validate_dict(self):
        d = {"gamma": 0.3, "L1": 100.0, "tp1": 5.0, "b1": 0.2, "a1": 0.4}
        p = RichardsPars.model_validate(d)
        assert p.a1 == 0.4


class TestSIRPars:
    def test_create(self):
        p = SIRPars(beta=0.6, gamma=0.3, R0=2.0, tc=5.0)
        assert p.R0 == 2.0


class TestFittedCurve:
    def test_create(self):
        fc = FittedCurve(
            ew=[Week(2024, 1), Week(2024, 2)],
            casos_cum=[10.0, 35.0],
            richards=[8.0, 30.0],
        )
        assert len(fc.ew) == 2
        assert fc.casos_cum == [10.0, 35.0]


class TestEpDuration:
    def test_create(self):
        ep = EpDuration(
            ini="202402", pw="202409", end="202412", dur=10, t_ini=1, t_end=11
        )
        assert ep.ini == "202402"
        assert ep.dur == 10

    def test_nan_fields(self):
        ep = EpDuration(
            ini=None,
            pw="202409",
            end=None,
            dur=0,
            t_ini=None,
            t_end=None,
        )
        assert ep.ini is None


class TestSirParams:
    def test_create(self):
        sp = SirParams(
            geocode=3550308,
            year=2024,
            ep_pw="202409",
            peak_week=8.0,
            beta=0.789,
            gamma=0.3,
            R0=2.63,
            total_cases=3354.0,
            alpha=0.62,
            sum_res=0.21,
        )
        assert sp.R0 == 2.63
        assert sp.ep_ini is None

    def test_full_create(self):
        sp = SirParams(
            geocode=1234567,
            year=2023,
            ep_ini="202301",
            ep_pw="202305",
            ep_end="202320",
            ep_dur=20,
            peak_week=5.0,
            beta=0.5,
            gamma=0.2,
            R0=2.5,
            total_cases=1000.0,
            alpha=0.4,
            sum_res=0.15,
            t_ini=0,
            t_end=20,
        )
        assert sp.ep_ini == "202301"
        assert sp.t_ini == 0


class TestParseAlerta:
    def test_from_dict(self):
        result = parse_alerta(
            {
                "SE": 202401,
                "casos_est": 10.5,
                "geocode": 3550308,
                "p_rt1": 0.95,
            }
        )
        assert len(result) == 1
        assert result[0].ew == Week(2024, 1)
        assert result[0].casos_est == 10.5

    def test_from_dict_with_data_iniSE(self):
        result = parse_alerta(
            {
                "data_iniSE": datetime(2024, 1, 1),
                "casos_est": 10.5,
                "geocode": 3550308,
                "p_rt1": 0.95,
            }
        )
        assert result[0].ew == Week(2024, 1)

    def test_from_dict_municipio_geocodigo(self):
        result = parse_alerta(
            {
                "SE": 202401,
                "casos_est": 10.5,
                "municipio_geocodigo": 3550308,
                "p_rt1": 0.95,
            }
        )
        assert result[0].geocode == 3550308

    def test_from_alerta_row_passthrough(self):
        row = AlertaRow(
            ew=Week(2024, 1), casos_est=10.5, geocode=3550308, p_rt1=0.95
        )
        result = parse_alerta(row)
        assert len(result) == 1
        assert result[0] is row

    def test_from_list_of_alerta_row(self):
        rows = [
            AlertaRow(ew=Week(2024, 1), casos_est=10.0, geocode=1, p_rt1=0.9),
            AlertaRow(ew=Week(2024, 2), casos_est=20.0, geocode=1, p_rt1=0.9),
        ]
        result = parse_alerta(rows)
        assert result is rows

    def test_from_list_of_dicts(self):
        result = parse_alerta(
            [
                {"SE": 202401, "casos_est": 10.0, "geocode": 1, "p_rt1": 0.9},
                {"SE": 202402, "casos_est": 20.0, "geocode": 2, "p_rt1": 0.9},
            ]
        )
        assert len(result) == 2
        assert result[0].ew == Week(2024, 1)
        assert result[1].ew == Week(2024, 2)

    def test_from_dataframe(self):
        import pandas as pd

        df = pd.DataFrame(
            {
                "SE": [202401, 202402],
                "casos_est": [10.0, 20.0],
                "municipio_geocodigo": [3550308, 3304557],
                "p_rt1": [0.95, 0.90],
            }
        )
        result = parse_alerta(df)
        assert len(result) == 2
        assert result[0].geocode == 3550308
        assert result[1].geocode == 3304557

    def test_invalid_type_raises(self):
        import pytest

        with pytest.raises(TypeError, match="Expected"):
            parse_alerta("invalid")
