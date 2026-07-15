from episcanner.scanner import EpiScanner
from episcanner.schemas import AlertaRow
from epiweeks import Week
import pytest


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


class TestEpiScannerEdgeCases:
    def test_empty_data_returns_empty(self):
        scanner = EpiScanner([], 2024)
        results = scanner.richards()
        assert results == []

    def test_empty_data_export_raises(self):
        scanner = EpiScanner([], 2024)
        with pytest.raises(ValueError, match="No data"):
            scanner.richards(export_to="csv", export_uf="SP")

    def test_no_transmission_returns_empty(self):
        data = [
            AlertaRow(
                ew=Week(2024, w), casos_est=1.0, geocode=3550308, p_rt1=0.1
            )
            for w in range(1, 53)
        ]
        scanner = EpiScanner(data, 2024)
        assert scanner.richards() == []

    def test_single_row_not_enough_for_transmission(self):
        data = [
            AlertaRow(
                ew=Week(2024, 30), casos_est=500.0, geocode=3550308, p_rt1=0.95
            )
        ]
        scanner = EpiScanner(data, 2024)
        assert scanner.richards() == []

    def test_dict_missing_casos_est_raises(self):
        with pytest.raises(KeyError):
            EpiScanner(
                [{"SE": 202401, "geocode": 3550308, "p_rt1": 0.95}], 2024
            )

    def test_dict_missing_p_rt1_raises(self):
        with pytest.raises(KeyError):
            EpiScanner(
                [{"SE": 202401, "casos_est": 10.0, "geocode": 3550308}], 2024
            )

    def test_dataframe_missing_columns_raises(self):
        import pandas as pd

        df = pd.DataFrame({"SE": [202401], "casos_est": [10.0]})
        with pytest.raises(KeyError):
            EpiScanner(df, 2024)

    def test_wrong_type_raises(self):
        with pytest.raises(TypeError, match="Expected"):
            EpiScanner("not valid data", 2024)

    def test_year_below_min_raises(self):
        data = _make_data()
        with pytest.raises(ValueError, match="Year must be"):
            EpiScanner(data, 2010)

    def test_export_to_invalid_format(self):
        data = _make_data()
        with pytest.raises(ValueError, match="Invalid format"):
            EpiScanner(data, 2024).richards(export_to="json", export_uf="SP")

    def test_geocode_too_small_from_dict(self):
        data = [
            {"SE": 202401, "casos_est": 10.0, "geocode": 123, "p_rt1": 0.95}
        ]
        scanner = EpiScanner(data, 2024)
        assert scanner.data[0].geocode == 123  # parse_alerta doesn't validate

    def test_zero_casos_est_returns_empty(self):
        data = [
            AlertaRow(
                ew=Week(2024, w), casos_est=0.0, geocode=3550308, p_rt1=0.95
            )
            for w in range(1, 53)
        ]
        scanner = EpiScanner(data, 2024)
        assert scanner.richards() == []

    def test_extreme_p_rt1_still_fits(self):
        data = [
            AlertaRow(
                ew=Week(2024, w),
                casos_est=c,
                geocode=3550308,
                p_rt1=0.99,
            )
            for w, c in enumerate(
                [10, 25, 60, 120, 200, 280, 340, 370, 390, 400, 405, 408],
                start=1,
            )
        ]
        scanner = EpiScanner(data, 2024)
        results = scanner.richards()
        assert len(results) == 1

    def test_negative_casos_est_fits(self):
        data = [
            AlertaRow(
                ew=Week(2024, w),
                casos_est=max(0.1, c),
                geocode=3550308,
                p_rt1=0.95,
            )
            for w, c in enumerate(
                [-5, 25, 60, 120, 200, 280, 340, 370, 390, 400, 405, 408],
                start=1,
            )
        ]
        scanner = EpiScanner(data, 2024)
        results = scanner.richards()
        assert len(results) == 1

    def test_export_overwrite_warning(self, tmp_path):
        data = _make_data()
        scanner = EpiScanner(data, 2024)
        scanner.richards(
            export_to="csv", export_uf="SP", export_output=str(tmp_path)
        )
        scanner.richards(
            export_to="csv", export_uf="SP", export_output=str(tmp_path)
        )

    def test_export_duckdb(self, tmp_path):
        data = _make_data()
        scanner = EpiScanner(data, 2024)
        results = scanner.richards(
            export_to="duckdb",
            export_uf="SP",
            export_output=str(tmp_path),
        )
        assert len(results) == 1
        import duckdb

        con = duckdb.connect(str(tmp_path / "episcanner.duckdb"))
        df = con.execute("SELECT * FROM SP").fetchdf()
        assert not df.empty
        con.close()

    def test_export_duckdb_upsert(self, tmp_path):
        data = _make_data()
        scanner = EpiScanner(data, 2024)
        scanner.richards(
            export_to="duckdb",
            export_uf="SP",
            export_output=str(tmp_path),
        )
        scanner.richards(
            export_to="duckdb",
            export_uf="SP",
            export_output=str(tmp_path),
        )
        import duckdb

        con = duckdb.connect(str(tmp_path / "episcanner.duckdb"))
        df = con.execute("SELECT * FROM SP").fetchdf()
        assert len(df) == 1
        con.close()

    def test_dict_with_string_date(self):
        data = [
            {
                "data_iniSE": "2024-01-07",
                "casos_est": 10.0,
                "geocode": 3550308,
                "p_rt1": 0.95,
            }
        ]
        scanner = EpiScanner(data, 2024)
        assert len(scanner.data) == 1

    def test_dict_cannot_derive_week_raises(self):
        import pytest

        with pytest.raises(ValueError, match="Cannot derive Week"):
            EpiScanner(
                [{"casos_est": 10.0, "geocode": 3550308, "p_rt1": 0.95}],
                2024,
            )
