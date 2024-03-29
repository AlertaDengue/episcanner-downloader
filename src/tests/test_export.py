import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
from scanner.scanner import EpiScanner


class TestEpiScanner(unittest.TestCase):
    @patch("scanner.scanner.EpiScanner._get_alerta_table")
    @patch("scanner.scanner.EpiScanner._scan_all_geocodes")
    def test_episcanner_with_mocked_results(
        self, mock_scan_all, mock_get_alerta_table
    ):
        mock_episcanner = MagicMock(spec=EpiScanner)

        csv = Path(__file__).parent / "AC_dengue_2010.csv"
        mock_df = pd.read_csv(csv)

        mock_episcanner.data = mock_df

        mock_episcanner.results = {
            1200013: [
                {
                    "success": True,
                    "params": {
                        "gamma": 0.3000087926384593,
                        "L1": 477.2352652000468,
                        "tp1": 9.813079238633112,
                        "b1": 0.15976248086695125,
                        "a1": 0.3474825202733575,
                    },
                    "sir_pars": {
                        "beta": 0.4597712735054106,
                        "gamma": 0.30000879263845937,
                        "R0": 1.5325259951946841,
                        "tc": 9.813079238633112,
                    },
                }
            ],
            1200401: [
                {
                    "success": False,
                    "params": {
                        "gamma": 0.30000000000000016,
                        "L1": 31757.92483379216,
                        "tp1": 15.701308253096638,
                        "b1": 0.10254509470875535,
                        "a1": 0.25474188123680286,
                    },
                    "sir_pars": {
                        "beta": 0.4025450947087555,
                        "gamma": 0.30000000000000016,
                        "R0": 1.3418169823625177,
                        "tc": 15.701308253096638,
                    },
                }
            ],
            1200450: [
                {
                    "success": True,
                    "params": {
                        "gamma": 0.30000000001538796,
                        "L1": 387.3309178034314,
                        "tp1": 34.99999999999784,
                        "b1": 0.04763272222507328,
                        "a1": 0.13702024918162112,
                    },
                    "sir_pars": {
                        "beta": 0.34763272224046127,
                        "gamma": 0.30000000001538796,
                        "R0": 1.1587757407421002,
                        "tc": 34.99999999999784,
                    },
                }
            ],
        }

        mock_episcanner.curves = {
            1200401: [
                {
                    "year": 2010,
                    "df": {
                        "data_iniSE": {
                            0: pd.Timestamp("2010-01-03 00:00:00"),
                            1: pd.Timestamp("2010-01-10 00:00:00"),
                            2: pd.Timestamp("2010-01-17 00:00:00"),
                            3: pd.Timestamp("2010-01-24 00:00:00"),
                            4: pd.Timestamp("2010-01-31 00:00:00"),
                            5: pd.Timestamp("2010-02-07 00:00:00"),
                            6: pd.Timestamp("2010-02-14 00:00:00"),
                            7: pd.Timestamp("2010-02-21 00:00:00"),
                            8: pd.Timestamp("2010-02-28 00:00:00"),
                            9: pd.Timestamp("2010-03-07 00:00:00"),
                            10: pd.Timestamp("2010-03-14 00:00:00"),
                            11: pd.Timestamp("2010-03-21 00:00:00"),
                            12: pd.Timestamp("2010-03-28 00:00:00"),
                            13: pd.Timestamp("2010-04-04 00:00:00"),
                            14: pd.Timestamp("2010-04-11 00:00:00"),
                            15: pd.Timestamp("2010-04-18 00:00:00"),
                            16: pd.Timestamp("2010-04-25 00:00:00"),
                            17: pd.Timestamp("2010-05-02 00:00:00"),
                            18: pd.Timestamp("2010-05-09 00:00:00"),
                            19: pd.Timestamp("2010-05-16 00:00:00"),
                            20: pd.Timestamp("2010-05-23 00:00:00"),
                            21: pd.Timestamp("2010-05-30 00:00:00"),
                            22: pd.Timestamp("2010-06-06 00:00:00"),
                            23: pd.Timestamp("2010-06-13 00:00:00"),
                            24: pd.Timestamp("2010-06-20 00:00:00"),
                            25: pd.Timestamp("2010-06-27 00:00:00"),
                            26: pd.Timestamp("2010-07-04 00:00:00"),
                            27: pd.Timestamp("2010-07-11 00:00:00"),
                            28: pd.Timestamp("2010-07-18 00:00:00"),
                            29: pd.Timestamp("2010-07-25 00:00:00"),
                            30: pd.Timestamp("2010-08-01 00:00:00"),
                            31: pd.Timestamp("2010-08-08 00:00:00"),
                            32: pd.Timestamp("2010-08-15 00:00:00"),
                            33: pd.Timestamp("2010-08-22 00:00:00"),
                            34: pd.Timestamp("2010-08-29 00:00:00"),
                            35: pd.Timestamp("2010-09-05 00:00:00"),
                            36: pd.Timestamp("2010-09-12 00:00:00"),
                            37: pd.Timestamp("2010-09-19 00:00:00"),
                            38: pd.Timestamp("2010-09-26 00:00:00"),
                            39: pd.Timestamp("2010-10-03 00:00:00"),
                            40: pd.Timestamp("2010-10-10 00:00:00"),
                            41: pd.Timestamp("2010-10-17 00:00:00"),
                            42: pd.Timestamp("2010-10-24 00:00:00"),
                            43: pd.Timestamp("2010-10-31 00:00:00"),
                            44: pd.Timestamp("2010-11-07 00:00:00"),
                            45: pd.Timestamp("2010-11-14 00:00:00"),
                            46: pd.Timestamp("2010-11-21 00:00:00"),
                            47: pd.Timestamp("2010-11-28 00:00:00"),
                            48: pd.Timestamp("2010-12-05 00:00:00"),
                            49: pd.Timestamp("2010-12-12 00:00:00"),
                            50: pd.Timestamp("2010-12-19 00:00:00"),
                            51: pd.Timestamp("2010-12-26 00:00:00"),
                        },
                        "casos": {
                            0: 680,
                            1: 841,
                            2: 829,
                            3: 1206,
                            4: 1789,
                            5: 1878,
                            6: 2077,
                            7: 1953,
                            8: 1989,
                            9: 1690,
                            10: 1540,
                            11: 1208,
                            12: 993,
                            13: 970,
                            14: 818,
                            15: 672,
                            16: 731,
                            17: 529,
                            18: 475,
                            19: 356,
                            20: 329,
                            21: 350,
                            22: 405,
                            23: 320,
                            24: 332,
                            25: 336,
                            26: 320,
                            27: 251,
                            28: 335,
                            29: 254,
                            30: 182,
                            31: 154,
                            32: 132,
                            33: 118,
                            34: 156,
                            35: 197,
                            36: 187,
                            37: 250,
                            38: 221,
                            39: 199,
                            40: 230,
                            41: 308,
                            42: 334,
                            43: 581,
                            44: 661,
                            45: 812,
                            46: 866,
                            47: 976,
                            48: 1209,
                            49: 1206,
                            50: 1652,
                            51: 1688,
                        },
                        "casos_cum": {
                            0: 680,
                            1: 1521,
                            2: 2350,
                            3: 3556,
                            4: 5345,
                            5: 7223,
                            6: 9300,
                            7: 11253,
                            8: 13242,
                            9: 14932,
                            10: 16472,
                            11: 17680,
                            12: 18673,
                            13: 19643,
                            14: 20461,
                            15: 21133,
                            16: 21864,
                            17: 22393,
                            18: 22868,
                            19: 23224,
                            20: 23553,
                            21: 23903,
                            22: 24308,
                            23: 24628,
                            24: 24960,
                            25: 25296,
                            26: 25616,
                            27: 25867,
                            28: 26202,
                            29: 26456,
                            30: 26638,
                            31: 26792,
                            32: 26924,
                            33: 27042,
                            34: 27198,
                            35: 27395,
                            36: 27582,
                            37: 27832,
                            38: 28053,
                            39: 28252,
                            40: 28482,
                            41: 28790,
                            42: 29124,
                            43: 29705,
                            44: 30366,
                            45: 31178,
                            46: 32044,
                            47: 33020,
                            48: 34229,
                            49: 35435,
                            50: 37087,
                            51: 38775,
                        },
                        "richards": {
                            0: 5625.028893790259,
                            1: 6154.892702727935,
                            2: 6726.330964038061,
                            3: 7340.917060421456,
                            4: 7999.926716478709,
                            5: 8704.25449475451,
                            6: 9454.323037696402,
                            7: 10249.987123456951,
                            8: 11090.435574163279,
                            9: 11974.095140636957,
                            10: 12898.541627031493,
                            11: 13860.424618001896,
                            12: 14855.413098686691,
                            13: 15878.169848431486,
                            14: 16922.362552663508,
                            15: 17980.71891828432,
                            16: 19045.131525855628,
                            17: 20106.815600497415,
                            18: 21156.51933554574,
                            19: 22184.782012075157,
                            20: 23182.230254968134,
                            21: 24139.897862178546,
                            22: 25049.55038788227,
                            23: 25903.99275976034,
                            24: 26697.337310739,
                            25: 27425.211149318195,
                            26: 28084.88589349747,
                            27: 28675.31915432388,
                            28: 29197.10506762406,
                            29: 29652.339608596638,
                            30: 30044.41421202686,
                            31: 30377.7572681008,
                            32: 30657.54657108024,
                            33: 30889.416412187442,
                            34: 31079.180871459317,
                            35: 31232.590552709178,
                            36: 31355.134380861502,
                            37: 31451.892089800494,
                            38: 31527.43752137294,
                            39: 31585.788449094904,
                            40: 31630.395655538177,
                            41: 31664.162468235834,
                            42: 31689.48571237003,
                            43: 31708.30975470361,
                            44: 31722.186633862715,
                            45: 31732.33686498206,
                            46: 31739.70710867538,
                            47: 31745.02232763845,
                            48: 31748.83122321169,
                            49: 31751.544619400483,
                            50: 31753.467059453385,
                            51: 31754.82224259742,
                        },
                    },
                    "residuals": {
                        0: 4945.028893790259,
                        1: 4633.892702727935,
                        2: 4376.330964038061,
                        3: 3784.917060421456,
                        4: 2654.926716478709,
                        5: 1481.2544947545102,
                        6: 154.32303769640203,
                        7: 1003.0128765430491,
                        8: 2151.564425836721,
                        9: 2957.904859363043,
                        10: 3573.4583729685073,
                        11: 3819.5753819981037,
                        12: 3817.5869013133088,
                        13: 3764.8301515685143,
                        14: 3538.637447336492,
                        15: 3152.2810817156787,
                        16: 2818.868474144372,
                        17: 2286.1843995025847,
                        18: 1711.4806644542587,
                        19: 1039.2179879248433,
                        20: 370.7697450318665,
                        21: 236.89786217854635,
                        22: 741.550387882271,
                        23: 1275.9927597603382,
                        24: 1737.3373107389998,
                        25: 2129.2111493181947,
                        26: 2468.8858934974705,
                        27: 2808.319154323879,
                        28: 2995.1050676240593,
                        29: 3196.339608596638,
                        30: 3406.414212026859,
                        31: 3585.7572681007987,
                        32: 3733.546571080238,
                        33: 3847.416412187442,
                        34: 3881.180871459317,
                        35: 3837.590552709178,
                        36: 3773.134380861502,
                        37: 3619.8920898004944,
                        38: 3474.43752137294,
                        39: 3333.7884490949036,
                        40: 3148.3956555381774,
                        41: 2874.1624682358342,
                        42: 2565.4857123700313,
                        43: 2003.3097547036086,
                        44: 1356.186633862715,
                        45: 554.3368649820586,
                        46: 304.29289132462145,
                        47: 1274.9776723615505,
                        48: 2480.1687767883086,
                        49: 3683.4553805995165,
                        50: 5333.532940546615,
                        51: 7020.17775740258,
                    },
                    "sum_res": 3.732232796155729,
                    "ep_time": {"ini": "201002", "end": "201040", "dur": 38},
                }
            ]
        }

        with patch("scanner.scanner.EpiScanner", return_value=mock_episcanner):
            x = EpiScanner("dengue", "AC", "2010")
            x.data = mock_episcanner.data
            x.results = mock_episcanner.results
            x.curves = mock_episcanner.curves

            expected_csv = Path("/tmp/AC_dengue_2010.csv")
            try:
                x.export(to="csv", output_dir="/tmp")
                self.assertTrue(expected_csv.exists())
                df = pd.read_csv(str(expected_csv.absolute()))
                self.assertFalse(df.empty)
                del df
            finally:
                expected_csv.unlink(missing_ok=True)

            expected_parquet = Path("/tmp/AC_dengue_2010.parquet")
            try:
                x.export(to="parquet", output_dir="/tmp")
                self.assertTrue(expected_parquet.exists())
                df = pd.read_parquet(str(expected_parquet.absolute()))
                self.assertFalse(df.empty)
                del df
            finally:
                expected_parquet.unlink(missing_ok=True)

            expected_duckdb = Path("/tmp/episcanner.duckdb")
            expected_schema = [
                ("disease", "STRING", None, None, None, None, None),
                ("CID10", "STRING", None, None, None, None, None),
                ("year", "NUMBER", None, None, None, None, None),
                ("geocode", "NUMBER", None, None, None, None, None),
                ("muni_name", "STRING", None, None, None, None, None),
                ("peak_week", "NUMBER", None, None, None, None, None),
                ("beta", "NUMBER", None, None, None, None, None),
                ("gamma", "NUMBER", None, None, None, None, None),
                ("R0", "NUMBER", None, None, None, None, None),
                ("total_cases", "NUMBER", None, None, None, None, None),
                ("alpha", "NUMBER", None, None, None, None, None),
                ("sum_res", "NUMBER", None, None, None, None, None),
                ("ep_ini", "STRING", None, None, None, None, None),
                ("ep_end", "STRING", None, None, None, None, None),
                ("ep_dur", "NUMBER", None, None, None, None, None),
            ]
            expected_result = {
                "disease": {0: "dengue"},
                "CID10": {0: "A90"},
                "year": {0: 2010},
                "geocode": {0: 1200401},
                "muni_name": {0: "Rio Branco"},
                "peak_week": {0: 15.701308253096638},
                "beta": {0: 0.4025450947087555},
                "gamma": {0: 0.30000000000000016},
                "R0": {0: 1.3418169823625177},
                "total_cases": {0: 31757.92483379216},
                "alpha": {0: 0.25474188123680286},
                "sum_res": {0: 3.732232796155729},
                "ep_ini": {0: "201002"},
                "ep_end": {0: "201040"},
                "ep_dur": {0: 38},
            }

            try:
                x.export(to="duckdb", output_dir="/tmp")
                self.assertTrue(expected_duckdb.exists())
                con = duckdb.connect(str(expected_duckdb.absolute()))
                result = con.execute("SELECT * FROM AC")

                schema = result.description
                df = result.fetchdf()

                self.assertFalse(df.empty)
                self.assertEqual(schema, expected_schema)
                self.assertEqual(df.to_dict(), expected_result)
                del df

            finally:
                expected_duckdb.unlink(missing_ok=True)
