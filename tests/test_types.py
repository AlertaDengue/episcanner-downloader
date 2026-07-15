from episcanner.types import CID10, UF, Disease, ExportFormat, Geocode, Year
import pytest


class TestDisease:
    def test_valid_dengue(self):
        assert Disease.__metadata__[0].func("dengue") == "dengue"

    def test_valid_zika(self):
        assert Disease.__metadata__[0].func("zika") == "zika"

    def test_valid_chik(self):
        assert Disease.__metadata__[0].func("chik") == "chik"

    def test_chikungunya_normalises_to_chik(self):
        assert Disease.__metadata__[0].func("Chikungunya") == "chik"

    def test_uppercase_normalises(self):
        assert Disease.__metadata__[0].func("DENGUE") == "dengue"

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid disease"):
            Disease.__metadata__[0].func("covid")

    def test_cid10_mapping(self):
        assert CID10["dengue"] == "A90"
        assert CID10["zika"] == "A92.8"
        assert CID10["chik"] == "A92.0"


class TestUF:
    def test_valid(self):
        assert UF.__metadata__[0].func("SP") == "SP"

    def test_lowercase_normalises(self):
        assert UF.__metadata__[0].func("rj") == "RJ"

    def test_all_27_states(self):
        func = UF.__metadata__[0].func
        for uf in [
            "AC",
            "AL",
            "AM",
            "AP",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MG",
            "MS",
            "MT",
            "PA",
            "PB",
            "PE",
            "PI",
            "PR",
            "RJ",
            "RN",
            "RO",
            "RR",
            "RS",
            "SC",
            "SE",
            "SP",
            "TO",
        ]:
            assert func(uf) == uf

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid UF"):
            UF.__metadata__[0].func("XX")


class TestYear:
    def test_valid(self):
        assert Year.__metadata__[0].func(2024) == 2024

    def test_min_year(self):
        assert Year.__metadata__[0].func(2011) == 2011

    def test_below_min_raises(self):
        with pytest.raises(ValueError, match="Year must be"):
            Year.__metadata__[0].func(2010)


class TestGeocode:
    def test_valid_7_digit(self):
        assert Geocode.__metadata__[0].func(3550308) == 3550308

    def test_min_7_digit(self):
        assert Geocode.__metadata__[0].func(1_000_000) == 1_000_000

    def test_max_7_digit(self):
        assert Geocode.__metadata__[0].func(9_999_999) == 9_999_999

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="7-digit"):
            Geocode.__metadata__[0].func(123)

    def test_too_long_raises(self):
        with pytest.raises(ValueError, match="7-digit"):
            Geocode.__metadata__[0].func(12345678)


class TestExportFormat:
    def test_valid_csv(self):
        assert ExportFormat.__metadata__[0].func("csv") == "csv"

    def test_valid_parquet(self):
        assert ExportFormat.__metadata__[0].func("parquet") == "parquet"

    def test_valid_duckdb(self):
        assert ExportFormat.__metadata__[0].func("duckdb") == "duckdb"

    def test_uppercase_normalises(self):
        assert ExportFormat.__metadata__[0].func("CSV") == "csv"

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid format"):
            ExportFormat.__metadata__[0].func("json")
