from typing import Optional, Union, Literal

from utils import STATES


def historico_alerta(
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
