import asyncio
import os
from collections import defaultdict
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .utils import CACHEPATH, STATES, get_SIR_pars, otim


def make_connection() -> Engine:
    """
    Returns:
        db_engine: URI with driver connection.
    """

    load_dotenv()
    PSQL_URI = os.getenv("EPISCANNER_PSQL_URI")

    try:
        connection = create_engine(PSQL_URI)
    except ConnectionError as e:
        logger.error(
            "Missing or incorrect `EPISCANNER_PSQL_URI` variable. Try:\n"
            "export EPISCANNER_PSQL_URI="
            '"postgresql://[user]:[password]@[host]:[port]/[database]"'
        )
        raise e
    return connection


class EpiScanner:
    results = defaultdict(list)
    curves = defaultdict(list)

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

        self.disease = disease
        self.uf = uf
        self.verbose = verbose
        self.data = self._get_alerta_table()
        self.window = int(self.data.SE.max() % 100)

        asyncio.run(self._scan_all())

    def export(
        self,
        to: Literal["csv", "parquet", "duckdb"],
        output_dir: str = CACHEPATH,
    ):
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

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        file_name = self.uf + "_" + self.disease + "." + format
        file = output_dir / file_name

        df = self._parse_results()

        if file.exists() and format != "duckdb":
            logger.warning(f"Overriding {file}")
            file.unlink()

        try:
            match format:
                case "csv":
                    df.to_csv(file)
                case "parquet":
                    df.to_parquet(file)
                case "duckdb":
                    self._to_duckdb(output_dir)

            logger.info(f"Data exported successfully to {file}")
        except (FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Failed to write file: {e}")
        except Exception as e:
            raise ValueError(f"Unexpected error while writing file: {e}")

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
            WHERE municipio.uf=\'{state_name}\'
            ORDER BY "data_iniSE" DESC;
        """

        with make_connection().connect() as conn:
            df = pd.read_sql_query(query, conn, index_col="id")

        df.data_iniSE = pd.to_datetime(df.data_iniSE)
        df.set_index("data_iniSE", inplace=True)
        return df

    def _filter_city(self, geocode):
        dfcity = self.data[self.data.municipio_geocodigo == geocode].copy()
        dfcity.sort_index(inplace=True)
        dfcity["casos_cum"] = dfcity.casos.cumsum()
        return dfcity

    def _save_results(self, geocode, year, results, curve):
        self.results[geocode].append(
            {
                "year": year,
                "success": results.success,
                "params": results.params.valuesdict(),
                "sir_pars": get_SIR_pars(results.params.valuesdict()),
            }
        )
        self.curves[geocode].append({"year": year, "df": curve})

    def _parse_results(self) -> pd.DataFrame:
        data = {
            "geocode": [],
            "year": [],
            "peak_week": [],
            "beta": [],
            "gamma": [],
            "R0": [],
            "total_cases": [],
            "alpha": [],
        }

        for gc, curve in self.curves.items():
            for c in curve:
                data["geocode"].append(gc)
                data["year"].append(c["year"])
                params = [
                    p["params"]
                    for p in self.results[gc]
                    if p["year"] == c["year"]
                ][0]
                sir_params = [
                    p["sir_pars"]
                    for p in self.results[gc]
                    if p["year"] == c["year"]
                ][0]
                data["peak_week"].append(params["tp1"])
                data["total_cases"].append(params["L1"])
                data["alpha"].append(params["a1"])
                data["beta"].append(sir_params["beta"])
                data["gamma"].append(sir_params["gamma"])
                data["R0"].append(sir_params["R0"])

        return pd.DataFrame(data)

    async def _scan(self, geocode):
        df = self._filter_city(geocode)
        df = df.assign(year=[i.year for i in df.index])

        async def scan_year(y):
            if self.verbose:
                logger.info(f"Scanning year {y}")

            dfy = df[df.year == y]
            has_transmission = dfy.transmissao.sum() > 3

            if not has_transmission:
                if self.verbose:
                    logger.info(
                        f"""
                        There were less than 3 weeks with Rt>1
                        in {geocode} in {y}.\nSkipping analysis
                        """
                    )
                return

            out, curve = otim(
                dfy[["casos", "casos_cum"]].iloc[0: self.window],  # NOQA E203
                0,
                self.window,
            )

            self._save_results(geocode, y, out, curve)

            if out.success:
                if self.verbose:
                    logger.info(
                        f"""
                            R0 in {y}: {
                            self.results[geocode][-1]['sir_pars']['R0']
                        }
                        """
                    )

        tasks = [scan_year(y) for y in set(df.year.values)]
        await asyncio.gather(*tasks)

    async def _scan_all(self):
        tasks = [
            self._scan(geocode)
            for geocode in self.data.municipio_geocodigo.unique()
        ]
        await asyncio.gather(*tasks)

    def _to_duckdb(self, output_dir: str):
        output_dir = Path(output_dir)
        db = output_dir / "episcanner.duckdb"
        con = duckdb.connect(str(db.absolute()))

        df = self._parse_results()
        table_name = self.uf

        con.register('df', df)
        con.execute(
            f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df'
        )
        con.unregister('df')
        con.close()

        if self.verbose:
            logger.info(f"{self.uf} data wrote into {db.absolute()}")
