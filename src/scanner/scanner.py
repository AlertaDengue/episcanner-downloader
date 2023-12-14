import os
from collections import defaultdict
from pathlib import Path
from typing import Optional, Literal
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
from loguru import logger

from .utils import otim, get_SIR_pars, STATES


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
    disease: Literal["dengue", "zika", "chik", "chikungunya"]
    uf: Literal[
        "AC", "AL", "AP", "AM", "BA", "CE", "ES", "GO", "MA", "MT", "MS", "MG",
        "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP",
        "SE", "TO", "DF"
    ]
    window: int
    results = defaultdict(list)
    curves = defaultdict(list)

    def __init__(
        self,
        last_week: int,
        disease: Literal["dengue", "zika", "chik", "chikungunya"],
        uf: Literal[
            "AC", "AL", "AP", "AM", "BA", "CE", "ES", "GO", "MA", "MT", "MS",
            "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR",
            "SC", "SP", "SE", "TO", "DF"
        ],
        verbose: bool = False
    ):
        """
        Detecting Epidemic Curves by Scanning Time Series Data

        Parameters
        ----------
        last_week : int
            The last week of data to include in the analysis, represented as
            a two-digit number (e.g., 20 for the 20th week of the year).
        disease: name of disease {'dengue', 'chik', 'zika'}
        uf: abbreviated codes of the federative units of Brazil. E.g: "SP"
        data : pandas.DataFrame
            A pandas DataFrame containing the time series data for all cities.
        """
        disease = disease.lower()

        if disease == "chikungunya":
            disease = "chik"

        if disease not in ["dengue", "zika", "chik"]:
            raise NotImplementedError(
                "Unknown `disease`. Options: dengue, zika, chikungunya"
            )

        uf = uf.upper()

        if uf not in STATES:
            raise NotImplementedError(
                f"Unknown `uf`. Options: {list(STATES.keys())}"
            )

        self.disease = disease
        self.uf = uf
        self.window = int(last_week)
        self.verbose = verbose
        self.data = self._get_alerta_table()

        for geocode in self.data.municipio_geocodigo.unique():
            self._scan(geocode)

    def to_csv(self, output_path: str) -> Path:
        """
        Parameters
        ----------
        output_path: entire or relative path of the result CSV file

        Return
        ----------
        Path of the CSV file
        """
        return self._to_csv(output_path)

    def to_parquet(self, output_path: str):
        """
        Parameters
        ----------
        output_path: entire or relative path of the result parquet file

        Return
        ----------
        Path of the parquet file
        """
        return self._to_parquet(output_path)

    def to_duckdb(self, output_path: str):
        ...

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

    def _scan(self, geocode):
        df = self._filter_city(geocode)
        df = df.assign(year=[i.year for i in df.index])
        for y in set(df.year.values):
            if self.verbose:
                logger.info(f"Scanning year {y}")
            dfy = df[df.year == y]
            has_transmission = dfy.transmissao.sum() > 3
            if not has_transmission:
                if self.verbose:
                    logger.info(
                        f"""
                        There where less that 3 weeks with Rt>1
                        in {geocode} in {y}.\nSkipping analysis
                        """
                    )
                continue
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

    def _to_csv(self, fname_path: str):
        dfpars = self._parse_results()
        fname_path = Path(fname_path)

        if fname_path.is_dir():
            raise ValueError(f"{fname_path} is a Directory")

        if fname_path.suffix.lower() != ".csv":
            raise ValueError(f"{fname_path} must a CSV file")

        if fname_path.exists():
            logger.warning(f"{fname_path} already exists. Skipping...")
            return fname_path

        try:
            fname_path.parent.mkdir(parents=True, exist_ok=True)
            dfpars.to_csv(fname_path)
            logger.info(f"Data exported successfully to {fname_path}")
        except (FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Failed to write CSV file: {e}")
        except Exception as e:
            raise ValueError(f"Unexpected error while writing CSV file: {e}")

        return fname_path

    def _to_parquet(self, fname_path: str):
        dfpars = self._parse_results()
        fname_path = Path(fname_path)

        if fname_path.is_dir():
            raise ValueError(f"{fname_path} is a Directory")

        if fname_path.suffix.lower() != ".parquet":
            raise ValueError(f"{fname_path} must a parquet file")

        if fname_path.exists():
            logger.warning(f"{fname_path} already exists. Skipping...")
            return fname_path

        try:
            fname_path.parent.mkdir(parents=True, exist_ok=True)
            dfpars.to_parquet(fname_path)
            logger.info(f"Data exported successfully to {fname_path}")
        except (FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Failed to write parquet file: {e}")
        except Exception as e:
            raise ValueError(
                f"Unexpected error while writing parquet file: {e}"
            )

        return fname_path
