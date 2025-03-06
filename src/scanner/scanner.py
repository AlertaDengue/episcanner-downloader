import asyncio
import os
from collections import defaultdict
from datetime import timedelta

# from datetime import datetime
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd
from dotenv import load_dotenv
from duckdb import BinderException, CatalogException
from epiweeks import Week
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .config import CUM_CASES, MIN_YEAR, N_WEEKS, THR_PROB
from .utils import (
    CACHEPATH,
    CID10,
    STATES,
    comp_duration,
    get_municipality_name,
    get_SIR_pars,
    otim,
)


def make_connection() -> Engine:
    """
    Returns:
        db_engine: URI with driver connection.
    """

    load_dotenv()
    PSQL_URI = os.getenv("EPISCANNER_PSQL_URI")

    try:
        connection = create_engine(PSQL_URI)
    except (ConnectionError, AttributeError):
        raise EnvironmentError(
            "Missing or incorrect `EPISCANNER_PSQL_URI` variable. Try:\n"
            "export EPISCANNER_PSQL_URI="
            '"postgresql://[user]:[password]@[host]:[port]/[database]"'
        )
    return connection


class EpiScanner:
    def __init__(
        self,
        disease: Literal["dengue", "zika", "chik", "chikungunya"],
        # fmt: off
        uf: Literal[
            "AC", "AL", "AP", "AM", "BA", "CE", "ES", "GO", "MA", "MT", "MS",
            "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR",
            "SC", "SP", "SE", "TO", "DF"
        ],
        # fmt: on
        year: int,
        verbose: bool = False,
    ):
        """
        Detecting Epidemic Curves by Scanning Time Series Data

        Parameters
        ----------
        disease: name of disease {'dengue', 'chik', 'zika'}
        uf: abbreviated codes of the federative units of Brazil. E.g: "SP"
        """
        disease = disease.lower()

        if disease == "chikungunya":
            disease = "chik"

        if disease not in ["dengue", "zika", "chik"]:
            raise NotImplementedError(
                f"Unknown disease. {disease}"
                "Options: dengue, zika, chikungunya"
            )

        uf = uf.upper()
        if uf not in STATES:
            raise NotImplementedError(
                f"Unknown uf {uf}. Options: {list(STATES.keys())}"
            )

        # cur_year = datetime.now().year
        year = int(year)
        if year < MIN_YEAR:
            raise ValueError(f"Year must be > {MIN_YEAR}")

        self.results = defaultdict(list)
        self.curves = defaultdict(list)
        self.disease = disease
        self.uf = uf
        self.year = year
        self.verbose = verbose
        self.data = self._get_alerta_table()

        asyncio.run(self._scan_all_geocodes())

    def to_dataframe(self) -> pd.DataFrame:
        return self._parse_results()

    def export(
        self,
        to: Literal["csv", "parquet", "duckdb"],
        output_dir: str = CACHEPATH,
    ) -> str:
        """
        Exports the result of the Scan into a file with the format:
        [UF]_[disease].[format]

        Parameters
        -------
            to: File format of the exported data. Options: duckdb, csv, parquet
            output_dir: the directory where the file will be exported
        """
        format = to

        if not bool(self.results):
            raise ValueError("No data to export")

        if format not in ["csv", "parquet", "duckdb"]:
            raise ValueError(
                f"Unknown output format type {format}. "
                "Options: csv, parquet or duckdb"
            )

        output_dir: Path = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        file_name = (
            self.uf + "_" + self.disease + "_" + str(self.year) + "." + format
        )
        file = output_dir / file_name

        df = self._parse_results()

        if file.exists() and format != "duckdb":
            logger.warning(f"Overriding {file}")
            file.unlink()

        try:
            if format == "csv":
                df.to_csv(file)

            if format == "parquet":
                df.to_parquet(file)

            if format == "duckdb":
                file = self._to_duckdb(df, str(output_dir.absolute()))

            logger.info(f"{self.uf} data for {self.year} wrote to {file}")
        except (FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Failed to write file: {e}")
        except Exception as e:
            raise ValueError(f"Unexpected error while writing file: {e}")

        return str(file.absolute())

    def _get_alerta_table(self) -> pd.DataFrame:
        """
        Pulls the HistoricoAlerta data for a disease and UF from the InfoDengue
        database.

        Returns
        -------
            df: Pandas dataframe
        """

        if self.disease == "dengue":
            table_suffix = ""
        else:
            table_suffix = "_" + self.disease

        state_name = STATES[self.uf]

        query = f"""
            SELECT historico.*
            FROM "Municipio"."Historico_alerta{table_suffix}" historico
            JOIN "Dengue_global"."Municipio" municipio
            ON historico.municipio_geocodigo=municipio.geocodigo
            WHERE municipio.uf='{state_name}'
            AND EXTRACT(YEAR FROM "data_iniSE") IN ({self.year-1}, {self.year})
            ORDER BY "data_iniSE" DESC;
        """

        with make_connection().connect() as conn:
            data = conn.execute(query).fetchall()
            df = pd.DataFrame(data)

        df.data_iniSE = pd.to_datetime(df.data_iniSE)
        df.set_index("data_iniSE", inplace=True, drop=True)
        return df

    def _filter_city(self, geocode):
        dfcity = self.data[self.data.municipio_geocodigo == geocode].copy()
        dfcity.sort_index(inplace=True)
        return dfcity

    def _save_results(self, geocode, results, curve):
        self.results[geocode].append(
            {
                "success": results.success,
                "params": results.params.valuesdict(),
                "sir_pars": get_SIR_pars(results.params.valuesdict()),
            }
        )
        self.curves[geocode].append(
            {
                "year": self.year,
                "df": curve,
                "residuals": abs(curve.richards - curve.casos_cum),
                "sum_res": (
                    sum(abs(curve.richards - curve.casos_cum))
                    / max(curve.casos_cum)
                ),
                "ep_time": comp_duration(
                    curve, results.params.valuesdict()["tp1"]
                ),
            }
        )

    def _parse_results(self) -> pd.DataFrame:
        data = {
            "disease": [],
            "CID10": [],
            "year": [],
            "geocode": [],
            "muni_name": [],
            "peak_week": [],
            "beta": [],
            "gamma": [],
            "R0": [],
            "total_cases": [],
            "alpha": [],
            "sum_res": [],
            "ep_ini": [],
            "ep_pw": [],
            "ep_end": [],
            "ep_dur": [],
            "t_ini": [],
            "t_end": [],
        }

        for gc, curve in self.curves.items():
            for c in curve:
                data["disease"].append(self.disease)
                data["CID10"].append(CID10[self.disease])
                data["geocode"].append(gc)
                data["muni_name"].append(get_municipality_name(gc))
                data["year"].append(c["year"])
                params = [p["params"] for p in self.results[gc]][0]
                sir_params = [p["sir_pars"] for p in self.results[gc]][0]
                data["peak_week"].append(params["tp1"])
                data["total_cases"].append(params["L1"])
                data["alpha"].append(params["a1"])
                data["beta"].append(sir_params["beta"])
                data["gamma"].append(sir_params["gamma"])
                data["R0"].append(sir_params["R0"])
                data["sum_res"].append(c["sum_res"])

                ep_duration = c["ep_time"]
                data["t_ini"].append(ep_duration["t_ini"])
                data["t_end"].append(ep_duration["t_end"])
                data["ep_ini"].append(ep_duration["ini"])
                data["ep_pw"].append(ep_duration["pw"])
                data["ep_end"].append(ep_duration["end"])
                data["ep_dur"].append(ep_duration["dur"])

        return pd.DataFrame(data)

    async def _scan(self, geocode):
        df = self._filter_city(geocode)
        df = df.assign(year=[i.year for i in df.index])

        dfy = df[
            (df.index >= f"{self.year-1}-11-01")
            & (df.index <= f"{self.year}-09-01")
        ]

        # has_transmission = dfy.transmissao.sum() > 3
        has_transmission = (
            (dfy.p_rt1 > THR_PROB).astype(int).sum() > N_WEEKS
        ) & (dfy.casos_est.sum() > CUM_CASES)

        if not has_transmission:
            if self.verbose:
                logger.info(
                    f"""
                    There were less than {N_WEEKS} weeks in which the
                    probability of the Rt>1 was bigger than {THR_PROB},
                    or less than {CUM_CASES} cumulative cases
                    between November of last year
                    and September in {geocode}.\nSkipping analysis
                    """
                )
            return

        df_otim = df[
            (df.year == self.year - 1) | (df.year == self.year)
        ].sort_index()

        start_date = pd.to_datetime(Week(self.year - 1, 45).startdate())
        end_date = pd.to_datetime(
            Week(self.year - 1, 45).startdate() + timedelta(weeks=52)
        )
        df_otim = df_otim.loc[
            (df_otim.index >= start_date) & (df_otim.index < end_date)
        ]

        out, curve = otim(
            df_otim,  # NOQA E203
        )

        self._save_results(geocode, out, curve)

        if out.success:
            if self.verbose:
                logger.info(
                    f"""
                        R0: {
                        self.results[geocode][-1]['sir_pars']['R0']
                    }
                    """
                )

    async def _scan_all_geocodes(self):
        tasks = []
        for geocode in self.data.municipio_geocodigo.unique():
            tasks.append(self._scan(geocode))
        await asyncio.gather(*tasks)

    def _to_duckdb(self, df: pd.DataFrame, output_dir: str) -> Path:
        db = Path(output_dir) / "episcanner.duckdb"
        con = duckdb.connect(str(db.absolute()))

        try:
            con.register("data", df)

            try:
                rows = con.execute(
                    f"SELECT COUNT(*) FROM '{self.uf}'"
                    f" WHERE year = {self.year} AND disease = '{self.disease}'"
                ).fetchone()[0]

                if rows > 0:
                    if self.verbose:
                        logger.warning(f"Overriding data for {self.year}")
                    con.execute(
                        f"DELETE FROM '{self.uf}'"
                        f" WHERE year = {self.year}"
                        f" AND disease = '{self.disease}'"
                    )
                con.execute(f"INSERT INTO '{self.uf}' SELECT * FROM data")
            except (CatalogException, BinderException):
                # table doesn't exist
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS '{self.uf}' "
                    "AS SELECT * FROM data"
                )
        finally:
            con.unregister("data")
            con.close()
        return db
