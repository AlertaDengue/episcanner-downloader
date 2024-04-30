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

        csv = Path(__file__).parent / "AC_dengue_2011.csv"
        mock_df = pd.read_csv(csv)

        mock_episcanner.data = mock_df

        mock_episcanner.results = {
            1200013: [
                {
                    "success": True,
                    "params": {
                        "gamma": 0.3085007465439495,
                        "L1": 379.25092524387463,
                        "tp1": 8.127733841218854,
                        "b1": 0.19405867348226216,
                        "a1": 0.38614075420602156,
                    },
                    "sir_pars": {
                        "beta": 0.5025594200262117,
                        "gamma": 0.30850074654394954,
                        "R0": 1.6290379380155444,
                        "tc": 8.127733841218854,
                    },
                }
            ],
            1200401: [
                {
                    "success": True,
                    "params": {
                        "gamma": 0.30000000000000016,
                        "L1": 34087.409590167095,
                        "tp1": 12.211368251771129,
                        "b1": 0.20929469128019657,
                        "a1": 0.4109500744138911,
                    },
                    "sir_pars": {
                        "beta": 0.5092946912801968,
                        "gamma": 0.3000000000000002,
                        "R0": 1.6976489709339881,
                        "tc": 12.211368251771129,
                    },
                }
            ],
            1200450: [
                {
                    "success": True,
                    "params": {
                        "gamma": 0.3000400798512347,
                        "L1": 803.882522052391,
                        "tp1": 11.477129499584349,
                        "b1": 0.21283590422562776,
                        "a1": 0.414985124734815,
                    },
                    "sir_pars": {
                        "beta": 0.5128759840768624,
                        "gamma": 0.30004007985123465,
                        "R0": 1.7093582441757638,
                        "tc": 11.477129499584349,
                    },
                }
            ],
        }

        mock_episcanner.curves = {
            1200401: [
                {
                    "year": 2011,
                    "df": {
                        "data_iniSE": {
                            0: pd.Timestamp("2010-11-07 00:00:00"),
                            1: pd.Timestamp("2010-11-14 00:00:00"),
                            2: pd.Timestamp("2010-11-21 00:00:00"),
                            3: pd.Timestamp("2010-11-28 00:00:00"),
                            4: pd.Timestamp("2010-12-05 00:00:00"),
                            5: pd.Timestamp("2010-12-12 00:00:00"),
                            6: pd.Timestamp("2010-12-19 00:00:00"),
                            7: pd.Timestamp("2010-12-26 00:00:00"),
                            8: pd.Timestamp("2011-01-02 00:00:00"),
                            9: pd.Timestamp("2011-01-09 00:00:00"),
                            10: pd.Timestamp("2011-01-16 00:00:00"),
                            11: pd.Timestamp("2011-01-23 00:00:00"),
                            12: pd.Timestamp("2011-01-30 00:00:00"),
                            13: pd.Timestamp("2011-02-06 00:00:00"),
                            14: pd.Timestamp("2011-02-13 00:00:00"),
                            15: pd.Timestamp("2011-02-20 00:00:00"),
                            16: pd.Timestamp("2011-02-27 00:00:00"),
                            17: pd.Timestamp("2011-03-06 00:00:00"),
                            18: pd.Timestamp("2011-03-13 00:00:00"),
                            19: pd.Timestamp("2011-03-20 00:00:00"),
                            20: pd.Timestamp("2011-03-27 00:00:00"),
                            21: pd.Timestamp("2011-04-03 00:00:00"),
                            22: pd.Timestamp("2011-04-10 00:00:00"),
                            23: pd.Timestamp("2011-04-17 00:00:00"),
                            24: pd.Timestamp("2011-04-24 00:00:00"),
                            25: pd.Timestamp("2011-05-01 00:00:00"),
                            26: pd.Timestamp("2011-05-08 00:00:00"),
                            27: pd.Timestamp("2011-05-15 00:00:00"),
                            28: pd.Timestamp("2011-05-22 00:00:00"),
                            29: pd.Timestamp("2011-05-29 00:00:00"),
                            30: pd.Timestamp("2011-06-05 00:00:00"),
                            31: pd.Timestamp("2011-06-12 00:00:00"),
                            32: pd.Timestamp("2011-06-19 00:00:00"),
                            33: pd.Timestamp("2011-06-26 00:00:00"),
                            34: pd.Timestamp("2011-07-03 00:00:00"),
                            35: pd.Timestamp("2011-07-10 00:00:00"),
                            36: pd.Timestamp("2011-07-17 00:00:00"),
                            37: pd.Timestamp("2011-07-24 00:00:00"),
                            38: pd.Timestamp("2011-07-31 00:00:00"),
                            39: pd.Timestamp("2011-08-07 00:00:00"),
                            40: pd.Timestamp("2011-08-14 00:00:00"),
                            41: pd.Timestamp("2011-08-21 00:00:00"),
                            42: pd.Timestamp("2011-08-28 00:00:00"),
                            43: pd.Timestamp("2011-09-04 00:00:00"),
                            44: pd.Timestamp("2011-09-11 00:00:00"),
                            45: pd.Timestamp("2011-09-18 00:00:00"),
                            46: pd.Timestamp("2011-09-25 00:00:00"),
                            47: pd.Timestamp("2011-10-02 00:00:00"),
                            48: pd.Timestamp("2011-10-09 00:00:00"),
                            49: pd.Timestamp("2011-10-16 00:00:00"),
                            50: pd.Timestamp("2011-10-23 00:00:00"),
                            51: pd.Timestamp("2011-10-30 00:00:00"),
                        },
                        "casos": {
                            0: 661,
                            1: 812,
                            2: 866,
                            3: 976,
                            4: 1209,
                            5: 1206,
                            6: 1652,
                            7: 1688,
                            8: 2461,
                            9: 2259,
                            10: 2319,
                            11: 2072,
                            12: 1981,
                            13: 1881,
                            14: 1606,
                            15: 1306,
                            16: 1292,
                            17: 1060,
                            18: 1058,
                            19: 1228,
                            20: 1081,
                            21: 984,
                            22: 702,
                            23: 515,
                            24: 376,
                            25: 313,
                            26: 235,
                            27: 171,
                            28: 111,
                            29: 115,
                            30: 75,
                            31: 72,
                            32: 54,
                            33: 44,
                            34: 45,
                            35: 48,
                            36: 51,
                            37: 31,
                            38: 41,
                            39: 42,
                            40: 30,
                            41: 15,
                            42: 31,
                            43: 30,
                            44: 32,
                            45: 30,
                            46: 44,
                            47: 33,
                            48: 21,
                            49: 31,
                            50: 41,
                            51: 55,
                        },
                        "casos_cum": {
                            0: 661,
                            1: 1473,
                            2: 2339,
                            3: 3315,
                            4: 4524,
                            5: 5730,
                            6: 7382,
                            7: 9070,
                            8: 11531,
                            9: 13790,
                            10: 16109,
                            11: 18181,
                            12: 20162,
                            13: 22043,
                            14: 23649,
                            15: 24955,
                            16: 26247,
                            17: 27307,
                            18: 28365,
                            19: 29593,
                            20: 30674,
                            21: 31658,
                            22: 32360,
                            23: 32875,
                            24: 33251,
                            25: 33564,
                            26: 33799,
                            27: 33970,
                            28: 34081,
                            29: 34196,
                            30: 34271,
                            31: 34343,
                            32: 34397,
                            33: 34441,
                            34: 34486,
                            35: 34534,
                            36: 34585,
                            37: 34616,
                            38: 34657,
                            39: 34699,
                            40: 34729,
                            41: 34744,
                            42: 34775,
                            43: 34805,
                            44: 34837,
                            45: 34867,
                            46: 34911,
                            47: 34944,
                            48: 34965,
                            49: 34996,
                            50: 35037,
                            51: 35092,
                        },
                        "richards": {
                            0: 2507.8949931146453,
                            1: 3054.235983517021,
                            2: 3709.588775700682,
                            3: 4490.9481944427025,
                            4: 5415.734515797169,
                            5: 6500.65891890477,
                            6: 7760.078037360563,
                            7: 9203.803241142265,
                            8: 10834.43618209257,
                            9: 12644.472292508533,
                            10: 14613.636527713097,
                            11: 16707.139329000147,
                            12: 18875.664935814584,
                            13: 21057.7966538628,
                            14: 23185.14263277039,
                            15: 25189.675151225703,
                            16: 27011.957408214563,
                            17: 28608.381974381475,
                            18: 29955.628960091715,
                            19: 31051.33700296015,
                            20: 31911.15336535085,
                            21: 32563.358655131255,
                            22: 33042.72140054669,
                            23: 33385.045085603386,
                            24: 33623.265659786586,
                            25: 33785.2930407527,
                            26: 33893.319032761596,
                            27: 33964.111951848354,
                            28: 34009.82765205217,
                            29: 34038.98410571532,
                            30: 34057.38592444676,
                            31: 34068.89902481476,
                            32: 34076.05009530448,
                            33: 34080.46520521455,
                            34: 34083.177655591135,
                            35: 34084.83729898992,
                            36: 34085.849387929586,
                            37: 34086.4649002763,
                            38: 34086.83839638167,
                            39: 34087.064623655664,
                            40: 34087.20144682464,
                            41: 34087.28409813858,
                            42: 34087.33397659911,
                            43: 34087.36405323131,
                            44: 34087.382177613974,
                            45: 34087.39309372621,
                            46: 34087.39966555971,
                            47: 34087.4036206272,
                            48: 34087.40600019717,
                            49: 34087.40743153913,
                            50: 34087.40829234941,
                            51: 34087.40880996321,
                        },
                    },
                    "residuals": {
                        0: 1846.8949931146453,
                        1: 1581.235983517021,
                        2: 1370.5887757006822,
                        3: 1175.9481944427025,
                        4: 891.7345157971686,
                        5: 770.65891890477,
                        6: 378.07803736056303,
                        7: 133.8032411422646,
                        8: 696.5638179074303,
                        9: 1145.5277074914666,
                        10: 1495.3634722869028,
                        11: 1473.860670999853,
                        12: 1286.3350641854158,
                        13: 985.2033461372012,
                        14: 463.85736722961155,
                        15: 234.6751512257033,
                        16: 764.9574082145627,
                        17: 1301.3819743814747,
                        18: 1590.6289600917153,
                        19: 1458.3370029601501,
                        20: 1237.1533653508486,
                        21: 905.3586551312546,
                        22: 682.7214005466885,
                        23: 510.0450856033858,
                        24: 372.2656597865862,
                        25: 221.29304075270193,
                        26: 94.31903276159574,
                        27: 5.888048151646217,
                        28: 71.17234794783144,
                        29: 157.01589428468287,
                        30: 213.61407555323967,
                        31: 274.1009751852398,
                        32: 320.94990469551703,
                        33: 360.5347947854534,
                        34: 402.8223444088653,
                        35: 449.1627010100783,
                        36: 499.15061207041435,
                        37: 529.5350997236965,
                        38: 570.1616036183332,
                        39: 611.935376344336,
                        40: 641.7985531753584,
                        41: 656.7159018614184,
                        42: 687.6660234008887,
                        43: 717.6359467686925,
                        44: 749.6178223860261,
                        45: 779.6069062737879,
                        46: 823.6003344402925,
                        47: 856.5963793728006,
                        48: 877.5939998028334,
                        49: 908.5925684608665,
                        50: 949.5917076505866,
                        51: 1004.5911900367864,
                    },
                    "sum_res": 1.1167343541101686,
                    "ep_time": {
                        "ini": "201046",
                        "pw": "201105",
                        "end": "201118",
                        "dur": 24,
                        "t_ini": 1,
                        "t_end": 25,
                    },
                }
            ]
        }

        with patch("scanner.scanner.EpiScanner", return_value=mock_episcanner):
            x = EpiScanner("dengue", "AC", "2011")
            x.data = mock_episcanner.data
            x.results = mock_episcanner.results
            x.curves = mock_episcanner.curves

            expected_csv = Path("/tmp/AC_dengue_2011.csv")
            try:
                x.export(to="csv", output_dir="/tmp")
                self.assertTrue(expected_csv.exists())
                df = pd.read_csv(str(expected_csv.absolute()))
                self.assertFalse(df.empty)
                del df
            finally:
                expected_csv.unlink(missing_ok=True)

            expected_parquet = Path("/tmp/AC_dengue_2011.parquet")
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
                ("ep_pw", "STRING", None, None, None, None, None),
                ("ep_end", "STRING", None, None, None, None, None),
                ("ep_dur", "NUMBER", None, None, None, None, None),
                ("t_ini", "NUMBER", None, None, None, None, None),
                ("t_end", "NUMBER", None, None, None, None, None),
            ]

            expected_result = {
                "disease": {0: "dengue"},
                "CID10": {0: "A90"},
                "year": {0: 2011},
                "geocode": {0: 1200401},
                "muni_name": {0: "Rio Branco"},
                "peak_week": {0: 12.211368251771129},
                "beta": {0: 0.5092946912801968},
                "gamma": {0: 0.3000000000000002},
                "R0": {0: 1.6976489709339881},
                "total_cases": {0: 34087.409590167095},
                "alpha": {0: 0.4109500744138911},
                "sum_res": {0: 1.1167343541101686},
                "ep_ini": {0: "201046"},
                "ep_pw": {0: "201105"},
                "ep_end": {0: "201118"},
                "ep_dur": {0: 24},
                "t_ini": {0: 1},
                "t_end": {0: 25},
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
