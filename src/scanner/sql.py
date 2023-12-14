import os
from dotenv import load_dotenv
from typing import Optional, Union, Literal

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .utils import STATES


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


def historico_alerta_query(
    disease: Literal["dengue", "zika", "chik", "chikungunya"],
    uf: Literal[
        "AC", "AL", "AP", "AM", "BA", "CE", "ES", "GO", "MA", "MT", "MS", "MG",
        "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP",
        "SE", "TO", "DF"
    ],
) -> str:
    """
    Returns a query for retrieving data from HistoricoAlerta[disease]
    """
    if not disease:
        raise ValueError(
            "`disease` not defined. Options: dengue, zika, chikungunya"
        )

    disease = disease.lower()

    if disease == "chikungunya":
        disease = "chik"

    if disease not in ["dengue", "zika", "chik"]:
        raise NotImplementedError(
            "Unknown `disease`. Options: dengue, zika, chikungunya"
        )

    if disease == "dengue":
        table_suffix = ""
    else:
        table_suffix = "_" + disease

    state_name = STATES[uf]
    return f"""
        SELECT historico.*
        FROM "Municipio"."Historico_alerta{table_suffix}" historico
        JOIN "Dengue_global"."Municipio" municipio
        ON historico.municipio_geocodigo=municipio.geocodigo
        WHERE municipio.uf=\'{state_name}\'
        ORDER BY "data_iniSE" DESC;"""
