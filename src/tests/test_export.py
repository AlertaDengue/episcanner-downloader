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
                        "gamma": 0.32128037862364717,
                        "L1": 80.9910739854064,
                        "tp1": 15.27406774133959,
                        "b1": 0.13597560088593993,
                        "a1": 0.2973730404395707,
                    },
                    "sir_pars": {
                        "beta": 0.457255979509587,
                        "gamma": 0.3212803786236471,
                        "R0": 1.423230330680181,
                        "tc": 15.27406774133959,
                    },
                }
            ],
            1200401: [
                {
                    "year": 2016,
                    "success": True,
                    "params": {
                        "gamma": 0.30000000000695465,
                        "L1": 1320.2972166942877,
                        "tp1": 9.717293540669154,
                        "b1": 0.3039176560775738,
                        "a1": 0.5032435349680112,
                    },
                    "sir_pars": {
                        "beta": 0.6039176560845285,
                        "gamma": 0.3000000000069547,
                        "R0": 2.0130588535684275,
                        "tc": 9.717293540669154,
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
                            "richards": 66.26963150575307,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-10 00:00:00"),
                            "casos": 26,
                            "casos_cum": 56,
                            "richards": 88.6067682077778,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-17 00:00:00"),
                            "casos": 22,
                            "casos_cum": 78,
                            "richards": 117.93509326804997,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-24 00:00:00"),
                            "casos": 28,
                            "casos_cum": 106,
                            "richards": 156.03341948156685,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-01-31 00:00:00"),
                            "casos": 49,
                            "casos_cum": 155,
                            "richards": 204.83330063265998,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-07 00:00:00"),
                            "casos": 85,
                            "casos_cum": 240,
                            "richards": 266.2062551523193,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-14 00:00:00"),
                            "casos": 80,
                            "casos_cum": 320,
                            "richards": 341.5942728456241,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-21 00:00:00"),
                            "casos": 143,
                            "casos_cum": 463,
                            "richards": 431.47672842329996,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-02-28 00:00:00"),
                            "casos": 125,
                            "casos_cum": 588,
                            "richards": 534.7486332477215,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-06 00:00:00"),
                            "casos": 112,
                            "casos_cum": 700,
                            "richards": 648.2119470414774,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-13 00:00:00"),
                            "casos": 82,
                            "casos_cum": 782,
                            "richards": 766.4926764265629,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-20 00:00:00"),
                            "casos": 89,
                            "casos_cum": 871,
                            "richards": 882.6661251036775,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-03-27 00:00:00"),
                            "casos": 85,
                            "casos_cum": 956,
                            "richards": 989.5970092224776,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-03 00:00:00"),
                            "casos": 89,
                            "casos_cum": 1045,
                            "richards": 1081.5658418946005,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-10 00:00:00"),
                            "casos": 72,
                            "casos_cum": 1117,
                            "richards": 1155.4809361585978,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-17 00:00:00"),
                            "casos": 64,
                            "casos_cum": 1181,
                            "richards": 1211.1569166751963,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-04-24 00:00:00"),
                            "casos": 28,
                            "casos_cum": 1209,
                            "richards": 1250.6749290047555,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-01 00:00:00"),
                            "casos": 30,
                            "casos_cum": 1239,
                            "richards": 1277.2930257702378,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-08 00:00:00"),
                            "casos": 15,
                            "casos_cum": 1254,
                            "richards": 1294.4404069675588,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-15 00:00:00"),
                            "casos": 16,
                            "casos_cum": 1270,
                            "richards": 1305.0873091988756,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-22 00:00:00"),
                            "casos": 9,
                            "casos_cum": 1279,
                            "richards": 1311.504708406991,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-05-29 00:00:00"),
                            "casos": 13,
                            "casos_cum": 1292,
                            "richards": 1315.2831775853315,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-05 00:00:00"),
                            "casos": 17,
                            "casos_cum": 1309,
                            "richards": 1317.4677312326128,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-12 00:00:00"),
                            "casos": 8,
                            "casos_cum": 1317,
                            "richards": 1318.7132193533953,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-19 00:00:00"),
                            "casos": 15,
                            "casos_cum": 1332,
                            "richards": 1319.4158118153596,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-06-26 00:00:00"),
                            "casos": 4,
                            "casos_cum": 1336,
                            "richards": 1319.8089882777078,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-03 00:00:00"),
                            "casos": 4,
                            "casos_cum": 1340,
                            "richards": 1320.0276944804323,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-10 00:00:00"),
                            "casos": 1,
                            "casos_cum": 1341,
                            "richards": 1320.1488057878091,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-17 00:00:00"),
                            "casos": 3,
                            "casos_cum": 1344,
                            "richards": 1320.2156488314529,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-24 00:00:00"),
                            "casos": 3,
                            "casos_cum": 1347,
                            "richards": 1320.2524489311684,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-07-31 00:00:00"),
                            "casos": 8,
                            "casos_cum": 1355,
                            "richards": 1320.272671753603,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-08-07 00:00:00"),
                            "casos": 3,
                            "casos_cum": 1358,
                            "richards": 1320.28376968462,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-08-14 00:00:00"),
                            "casos": 5,
                            "casos_cum": 1363,
                            "richards": 1320.2898538897343,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2016-08-21 00:00:00"),
                            "casos": 4,
                            "casos_cum": 1367,
                            "richards": 1320.2931869371841,
                        },
                    ],
                    "sum_res": 0.7772283922018478,
                    "ep_time": {"ini": "201602", "end": "201621", "dur": 19},
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
                            "richards": 9.376432206855938,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-01-12 00:00:00"),
                            "casos": 3,
                            "casos_cum": 3,
                            "richards": 10.62240303658875,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-01-19 00:00:00"),
                            "casos": 5,
                            "casos_cum": 8,
                            "richards": 12.015511681052189,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-01-26 00:00:00"),
                            "casos": 4,
                            "casos_cum": 12,
                            "richards": 13.567878222331458,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-02 00:00:00"),
                            "casos": 4,
                            "casos_cum": 16,
                            "richards": 15.29110890481219,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-09 00:00:00"),
                            "casos": 7,
                            "casos_cum": 23,
                            "richards": 17.195772262219727,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-16 00:00:00"),
                            "casos": 5,
                            "casos_cum": 28,
                            "richards": 19.290758070351373,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-02-23 00:00:00"),
                            "casos": 1,
                            "casos_cum": 29,
                            "richards": 21.582520533971575,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-01 00:00:00"),
                            "casos": 2,
                            "casos_cum": 31,
                            "richards": 24.07421918150152,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-08 00:00:00"),
                            "casos": 0,
                            "casos_cum": 31,
                            "richards": 26.764787417173586,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-15 00:00:00"),
                            "casos": 2,
                            "casos_cum": 33,
                            "richards": 29.647979363065083,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-22 00:00:00"),
                            "casos": 2,
                            "casos_cum": 35,
                            "richards": 32.71146930329226,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-03-29 00:00:00"),
                            "casos": 1,
                            "casos_cum": 36,
                            "richards": 35.93610210807722,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-05 00:00:00"),
                            "casos": 0,
                            "casos_cum": 36,
                            "richards": 39.295413186261335,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-12 00:00:00"),
                            "casos": 1,
                            "casos_cum": 37,
                            "richards": 42.75554691107714,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-19 00:00:00"),
                            "casos": 2,
                            "casos_cum": 39,
                            "richards": 46.275696250397615,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-04-26 00:00:00"),
                            "casos": 2,
                            "casos_cum": 41,
                            "richards": 49.80915712837047,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-03 00:00:00"),
                            "casos": 9,
                            "casos_cum": 50,
                            "richards": 53.305035089550344,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-10 00:00:00"),
                            "casos": 11,
                            "casos_cum": 61,
                            "richards": 56.710560542619675,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-17 00:00:00"),
                            "casos": 4,
                            "casos_cum": 65,
                            "richards": 59.97387093931943,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-24 00:00:00"),
                            "casos": 5,
                            "casos_cum": 70,
                            "richards": 63.04702023963422,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-05-31 00:00:00"),
                            "casos": 2,
                            "casos_cum": 72,
                            "richards": 65.88890016532741,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-07 00:00:00"),
                            "casos": 0,
                            "casos_cum": 72,
                            "richards": 68.46772680224859,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-14 00:00:00"),
                            "casos": 0,
                            "casos_cum": 72,
                            "richards": 70.76277608238665,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-21 00:00:00"),
                            "casos": 1,
                            "casos_cum": 73,
                            "richards": 72.76514489025757,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-06-28 00:00:00"),
                            "casos": 0,
                            "casos_cum": 73,
                            "richards": 74.47745617523961,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-05 00:00:00"),
                            "casos": 0,
                            "casos_cum": 73,
                            "richards": 75.91258685787069,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-12 00:00:00"),
                            "casos": 3,
                            "casos_cum": 76,
                            "richards": 77.09164016795434,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-19 00:00:00"),
                            "casos": 0,
                            "casos_cum": 76,
                            "richards": 78.04147760459507,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-07-26 00:00:00"),
                            "casos": 0,
                            "casos_cum": 76,
                            "richards": 78.79215266233771,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-02 00:00:00"),
                            "casos": 0,
                            "casos_cum": 76,
                            "richards": 79.3745503918314,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-09 00:00:00"),
                            "casos": 1,
                            "casos_cum": 77,
                            "richards": 79.8184517099112,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-16 00:00:00"),
                            "casos": 1,
                            "casos_cum": 78,
                            "richards": 80.15113575862448,
                        },
                        {
                            "data_iniSE": pd.Timestamp("2020-08-23 00:00:00"),
                            "casos": 0,
                            "casos_cum": 78,
                            "richards": 80.39653357630492,
                        },
                    ],
                    "sum_res": 1.7816454078320072,
                    "ep_time": {"ini": "202003", "end": "202035", "dur": 32},
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
