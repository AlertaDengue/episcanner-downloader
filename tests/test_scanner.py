from episcanner.scanner import EpiScanner
from episcanner.schemas import AlertaRow, SirParams
from epiweeks import Week


def _make_data():
    return [
        AlertaRow(
            ew=Week(2024, w),
            casos_est=c,
            geocode=3550308,
            p_rt1=0.95,
        )
        for w, c in enumerate(
            [10, 25, 60, 120, 200, 280, 340, 370, 390, 400, 405, 408],
            start=1,
        )
    ]


def _make_multi_geocode_data():
    rows = []
    for gc in (3550308, 3304557):
        for w, c in enumerate(
            [10, 25, 60, 120, 200, 280, 340, 370, 390, 400, 405, 408],
            start=1,
        ):
            rows.append(
                AlertaRow(
                    ew=Week(2024, w),
                    casos_est=c * (1 + 0.5 * (gc == 3304557)),
                    geocode=gc,
                    p_rt1=0.95,
                )
            )
    return rows


class TestEpiScanner:
    def test_richards_single_geocode(self):
        data = _make_data()
        scanner = EpiScanner(data, 2024)
        results = scanner.richards()
        assert len(results) == 1
        r = results[0]
        assert isinstance(r, SirParams)
        assert r.geocode == 3550308
        assert r.year == 2024
        assert r.R0 > 0
        assert r.peak_week > 0
        assert r.total_cases > 0
        assert r.ep_pw is not None

    def test_richards_multi_geocode(self):
        data = _make_multi_geocode_data()
        scanner = EpiScanner(data, 2024)
        results = scanner.richards()
        assert len(results) == 2
        geocodes = {r.geocode for r in results}
        assert geocodes == {3550308, 3304557}

    def test_no_transmission_skips(self):
        data = [
            AlertaRow(
                ew=Week(2024, w),
                casos_est=1.0,
                geocode=3550308,
                p_rt1=0.1,
            )
            for w in range(1, 13)
        ]
        scanner = EpiScanner(data, 2024)
        results = scanner.richards()
        assert len(results) == 0

    def test_richards_with_export_csv(self, tmp_path):
        data = _make_data()
        scanner = EpiScanner(data, 2024)
        results = scanner.richards(
            export_to="csv", export_uf="SP", export_output=str(tmp_path)
        )
        assert len(results) == 1
        import os

        assert os.path.exists(str(tmp_path / "SP_2024.csv"))

    def test_richards_with_export_parquet(self, tmp_path):
        data = _make_data()
        scanner = EpiScanner(data, 2024)
        results = scanner.richards(
            export_to="parquet",
            export_uf="SP",
            export_output=str(tmp_path),
        )
        assert len(results) == 1
        import os

        assert os.path.exists(str(tmp_path / "SP_2024.parquet"))

    def test_richards_export_no_results(self):
        data = [
            AlertaRow(
                ew=Week(2024, w),
                casos_est=1.0,
                geocode=3550308,
                p_rt1=0.1,
            )
            for w in range(1, 13)
        ]
        scanner = EpiScanner(data, 2024)
        import pytest

        with pytest.raises(ValueError, match="No data"):
            scanner.richards(
                export_to="csv", export_uf="SP", export_output="/tmp"
            )

    def test_from_dataframe(self):
        import pandas as pd

        df = pd.DataFrame(
            {
                "SE": [202401, 202402, 202403],
                "casos_est": [10.0, 25.0, 60.0],
                "municipio_geocodigo": [3550308, 3550308, 3550308],
                "p_rt1": [0.95, 0.92, 0.88],
            }
        )
        scanner = EpiScanner(df, 2024)
        assert len(scanner.data) == 3

    def test_from_dicts(self):
        data = [
            {
                "SE": 202401,
                "casos_est": 10.0,
                "geocode": 3550308,
                "p_rt1": 0.95,
            },
            {
                "SE": 202402,
                "casos_est": 25.0,
                "geocode": 3550308,
                "p_rt1": 0.92,
            },
        ]
        scanner = EpiScanner(data, 2024)
        assert len(scanner.data) == 2
        assert isinstance(scanner.data[0], AlertaRow)
