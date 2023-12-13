import os
from dotenv import load_dotenv
from typing import Optional, Union, Literal

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from utils import STATES


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
            "Missing or incorrect `EPISCANNER_PSQL_URI` variable. Try:"
        )
        logger.error(
            "export EPISCANNER_PSQL_URI="
            '"postgresql://[user]:[password]@[host]:[port]/[database]"'
        )
        raise e
    return connection


def historico_alerta_query(
    disease: Literal["dengue", "zika", "chik", "chikungunya"],
    # fmt: off
    uf: Optional[Literal[
        "AC", "AL", "AP", "AM", "BA", "CE", "ES", "GO", "MA", "MT", "MS", "MG",
        "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP",
        "SE", "TO", "DF"
    ]] = None,
    # fmt: on
    geocode: Optional[Union[str, int]] = None
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

    if (bool(uf) and bool(geocode)) or not (bool(uf) or bool(geocode)):
        raise ValueError(
            "Only and at least one of those shaw be defined: `uf` or `geocode`"
        )

    if uf:
        state_name = STATES[uf]
        return f"""
            SELECT historico.*
            FROM "Municipio"."Historico_alerta{table_suffix}" historico
            JOIN "Dengue_global"."Municipio" municipio
            ON historico.municipio_geocodigo=municipio.geocodigo
            WHERE municipio.uf=\'{state_name}\'
            ORDER BY "data_iniSE" DESC ;"""

    if geocode:
        return f"""
            SELECT *
            FROM "Municipio"."Historico_alerta{table_suffix}"
            WHERE municipio_geocodigo={geocode}
            ORDER BY "data_iniSE" DESC ;"""
