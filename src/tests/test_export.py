import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
from scanner.scanner import EpiScanner


class TestEpiScanner(unittest.TestCase):
    @patch("scanner.scanner.EpiScanner._get_alerta_table")
    @patch("scanner.scanner.EpiScanner._scan_all")
    def test_episcanner_with_mocked_results(
        self, mock_scan_all, mock_get_alerta_table
    ):
        mock_episcanner = MagicMock(spec=EpiScanner)

        mock_results = {
            1200203: [
                {
                    "year": 2020,
                    "success": True,
                    "params": {
                        "gamma": 0.9629379365808304,
                        "L1": 79.69455363808287,
                        "tp1": 15.734823855208194,
                        "b1": 0.13021750502845372,
                        "a1": 0.11912075819405388,
                    },
                    "sir_pars": {
                        "beta": 1.0931554416092841,
                        "gamma": 0.9629379365808304,
                        "R0": 1.1352293850742095,
                        "tc": 15.734823855208194,
                    },
                }
            ],
            1200401: [
                {
                    "year": 2016,
                    "success": True,
                    "params": {
                        "gamma": 0.9500000000079135,
                        "L1": 1312.6040366040406,
                        "tp1": 10.216855837226374,
                        "b1": 0.2753371480511123,
                        "a1": 0.2247031753564767,
                    },
                    "sir_pars": {
                        "beta": 1.2253371480590258,
                        "gamma": 0.9500000000079135,
                        "R0": 1.2898285768934934,
                        "tc": 10.216855837226374,
                    },
                }
            ],
        }

        mock_curves = {
            1200401: [
                {
                    "year": 2016,
                    "df": [
                        {
                            "data_iniSE": pd.Timestamp("2016-01-03 00:00:00"),
                            "casos": 30,
                            "casos_cum": 30,
                            "richards": 75.96869211402577,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-10 00:00:00"),
                            "casos": 26,
                            "casos_cum": 56,
                            "richards": 98.91788404108433,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-17 00:00:00"),
                            "casos": 22,
                            "casos_cum": 78,
                            "richards": 128.35116180542536,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-24 00:00:00"),
                            "casos": 28,
                            "casos_cum": 106,
                            "richards": 165.79245063768485,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-31 00:00:00"),
                            "casos": 49,
                            "casos_cum": 155,
                            "richards": 212.91603548272724,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-07 00:00:00"),
                            "casos": 85,
                            "casos_cum": 240,
                            "richards": 271.41406766550176,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-14 00:00:00"),
                            "casos": 80,
                            "casos_cum": 320,
                            "richards": 342.7553026887507,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-21 00:00:00"),
                            "casos": 143,
                            "casos_cum": 463,
                            "richards": 427.80854221019206,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-28 00:00:00"),
                            "casos": 125,
                            "casos_cum": 588,
                            "richards": 526.336006746427,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-06 00:00:00"),
                            "casos": 112,
                            "casos_cum": 700,
                            "richards": 636.4321733591823,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-13 00:00:00"),
                            "casos": 82,
                            "casos_cum": 782,
                            "richards": 754.0921668617001,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-20 00:00:00"),
                            "casos": 89,
                            "casos_cum": 871,
                            "richards": 873.199574108212,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-27 00:00:00"),
                            "casos": 85,
                            "casos_cum": 956,
                            "richards": 986.2254977744477,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-03 00:00:00"),
                            "casos": 89,
                            "casos_cum": 1045,
                            "richards": 1085.7063999855882,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-10 00:00:00"),
                            "casos": 72,
                            "casos_cum": 1117,
                            "richards": 1166.1126068845501,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-17 00:00:00"),
                            "casos": 64,
                            "casos_cum": 1181,
                            "richards": 1225.294618313825,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-24 00:00:00"),
                            "casos": 28,
                            "casos_cum": 1209,
                            "richards": 1264.7329666761407,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-01 00:00:00"),
                            "casos": 30,
                            "casos_cum": 1239,
                            "richards": 1288.4684011339486,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-08 00:00:00"),
                            "casos": 15,
                            "casos_cum": 1254,
                            "richards": 1301.3846376738998,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-15 00:00:00"),
                            "casos": 16,
                            "casos_cum": 1270,
                            "richards": 1307.769717530139,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-22 00:00:00"),
                            "casos": 9,
                            "casos_cum": 1279,
                            "richards": 1310.6588724954809,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-29 00:00:00"),
                            "casos": 13,
                            "casos_cum": 1292,
                            "richards": 1311.8669262688034,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-05 00:00:00"),
                            "casos": 17,
                            "casos_cum": 1309,
                            "richards": 1312.3386313765577,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-12 00:00:00"),
                            "casos": 8,
                            "casos_cum": 1317,
                            "richards": 1312.5124577554793,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-19 00:00:00"),
                            "casos": 15,
                            "casos_cum": 1332,
                            "richards": 1312.5735175212928,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-26 00:00:00"),
                            "casos": 4,
                            "casos_cum": 1336,
                            "richards": 1312.5941466427416,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-03 00:00:00"),
                            "casos": 4,
                            "casos_cum": 1340,
                            "richards": 1312.600902122935,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-10 00:00:00"),
                            "casos": 1,
                            "casos_cum": 1341,
                            "richards": 1312.6030603831207,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-17 00:00:00"),
                            "casos": 3,
                            "casos_cum": 1344,
                            "richards": 1312.6037366798203,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-24 00:00:00"),
                            "casos": 3,
                            "casos_cum": 1347,
                            "richards": 1312.603945426727,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-31 00:00:00"),
                            "casos": 8,
                            "casos_cum": 1355,
                            "richards": 1312.6040091110115,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-08-07 00:00:00"),
                            "casos": 3,
                            "casos_cum": 1358,
                            "richards": 1312.6040283657312,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-08-14 00:00:00"),
                            "casos": 5,
                            "casos_cum": 1363,
                            "richards": 1312.604034147246,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-08-21 00:00:00"),
                            "casos": 4,
                            "casos_cum": 1367,
                            "richards": 1312.60403587407,
                        },
                    ],
                }
            ],
            1200203: [
                {
                    "year": 2020,
                    "df": [
                        {
                            "data_iniSE": pd.Timestamp("2020-01-05 00:00:00"),
                            "casos": 0,
                            "casos_cum": 0,
                            "richards": 9.56739258463287,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-01-12 00:00:00"),
                            "casos": 3,
                            "casos_cum": 3,
                            "richards": 10.793124514517388,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-01-19 00:00:00"),
                            "casos": 5,
                            "casos_cum": 8,
                            "richards": 12.160206576313044,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-01-26 00:00:00"),
                            "casos": 4,
                            "casos_cum": 12,
                            "richards": 13.680564786682993,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-02 00:00:00"),
                            "casos": 4,
                            "casos_cum": 16,
                            "richards": 15.36588853907071,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-09 00:00:00"),
                            "casos": 7,
                            "casos_cum": 23,
                            "richards": 17.22720261079722,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-16 00:00:00"),
                            "casos": 5,
                            "casos_cum": 28,
                            "richards": 19.274327647839826,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-23 00:00:00"),
                            "casos": 1,
                            "casos_cum": 29,
                            "richards": 21.515220694223316,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-01 00:00:00"),
                            "casos": 2,
                            "casos_cum": 31,
                            "richards": 23.955194274980478,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-08 00:00:00"),
                            "casos": 0,
                            "casos_cum": 31,
                            "richards": 26.596023409937516,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-15 00:00:00"),
                            "casos": 2,
                            "casos_cum": 33,
                            "richards": 29.434965487503455,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-22 00:00:00"),
                            "casos": 2,
                            "casos_cum": 35,
                            "richards": 32.46373855171391,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-29 00:00:00"),
                            "casos": 1,
                            "casos_cum": 36,
                            "richards": 35.667528897012666,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-05 00:00:00"),
                            "casos": 0,
                            "casos_cum": 36,
                            "richards": 39.024127343067775,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-12 00:00:00"),
                            "casos": 1,
                            "casos_cum": 37,
                            "richards": 42.50332184424383,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-19 00:00:00"),
                            "casos": 2,
                            "casos_cum": 39,
                            "richards": 46.066696667745944,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-26 00:00:00"),
                            "casos": 2,
                            "casos_cum": 41,
                            "richards": 49.66799749559749,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-03 00:00:00"),
                            "casos": 9,
                            "casos_cum": 50,
                            "richards": 53.254208088104775,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-10 00:00:00"),
                            "casos": 11,
                            "casos_cum": 61,
                            "richards": 56.76743826638387,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-17 00:00:00"),
                            "casos": 4,
                            "casos_cum": 65,
                            "richards": 60.14763861503398,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-24 00:00:00"),
                            "casos": 5,
                            "casos_cum": 70,
                            "richards": 63.33603518723925,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-31 00:00:00"),
                            "casos": 2,
                            "casos_cum": 72,
                            "richards": 66.27902951186368,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-07 00:00:00"),
                            "casos": 0,
                            "casos_cum": 72,
                            "richards": 68.932160943845,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-14 00:00:00"),
                            "casos": 0,
                            "casos_cum": 72,
                            "richards": 71.2636176303161,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-21 00:00:00"),
                            "casos": 1,
                            "casos_cum": 73,
                            "richards": 73.25675149519483,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-28 00:00:00"),
                            "casos": 0,
                            "casos_cum": 73,
                            "richards": 74.91113509302824,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-05 00:00:00"),
                            "casos": 0,
                            "casos_cum": 73,
                            "richards": 76.24190194515353,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-12 00:00:00"),
                            "casos": 3,
                            "casos_cum": 76,
                            "richards": 77.27740729374054,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-19 00:00:00"),
                            "casos": 0,
                            "casos_cum": 76,
                            "richards": 78.05556556724088,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-26 00:00:00"),
                            "casos": 0,
                            "casos_cum": 76,
                            "richards": 78.61947608129029,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-02 00:00:00"),
                            "casos": 0,
                            "casos_cum": 76,
                            "richards": 79.01306178033126,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-09 00:00:00"),
                            "casos": 1,
                            "casos_cum": 77,
                            "richards": 79.27738196153378,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-16 00:00:00"),
                            "casos": 1,
                            "casos_cum": 78,
                            "richards": 79.44806340573705,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-23 00:00:00"),
                            "casos": 0,
                            "casos_cum": 78,
                            "richards": 79.55400046923637,
                        },
                    ],
                }
            ],
        }

        mock_episcanner.results = mock_results
        mock_episcanner.curves = mock_curves

        with patch("scanner.scanner.EpiScanner", return_value=mock_episcanner):
            x = EpiScanner("zika", "AC")
            x.results = mock_episcanner.results
            x.curves = mock_episcanner.curves

            expected_csv = Path("/tmp/AC_zika.csv")
            try:
                x.export(to="csv", output_dir="/tmp")
                self.assertTrue(expected_csv.exists())
                df = pd.read_csv(str(expected_csv.absolute()))
                self.assertFalse(df.empty)
                del df
            finally:
                expected_csv.unlink(missing_ok=True)

            expected_parquet = Path("/tmp/AC_zika.parquet")
            try:
                x.export(to="parquet", output_dir="/tmp")
                self.assertTrue(expected_parquet.exists())
                df = pd.read_parquet(str(expected_parquet.absolute()))
                self.assertFalse(df.empty)
                del df
            finally:
                expected_parquet.unlink(missing_ok=True)

            expected_duckdb = Path("/tmp/episcanner.duckdb")
            try:
                x.export(to="duckdb", output_dir="/tmp")
                self.assertTrue(expected_duckdb.exists())
                con = duckdb.connect(str(expected_duckdb.absolute()))
                result = con.execute("SELECT * FROM AC")
                df = result.fetchdf()
                self.assertFalse(df.empty)
                del df

            finally:
                expected_duckdb.unlink(missing_ok=True)
