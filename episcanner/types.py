from typing import Annotated

from pydantic import BeforeValidator

MIN_YEAR = 2011
_DISEASES = frozenset({"dengue", "zika", "chik"})
_UF_CODES = frozenset(
    {
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    }
)
_EXPORT_FORMATS = frozenset({"csv", "parquet", "duckdb"})

CID10 = {
    "dengue": "A90",
    "zika": "A92.8",
    "chik": "A92.0",
}


def _parse_disease(v: str) -> str:
    v = v.lower()
    if v == "chikungunya":
        v = "chik"
    if v not in _DISEASES:
        raise ValueError(
            f"Invalid disease '{v}'. Options: {sorted(_DISEASES)}"
        )
    return v


def _parse_uf(v: str) -> str:
    v = v.upper()
    if v not in _UF_CODES:
        raise ValueError(f"Invalid UF '{v}'. Options: {sorted(_UF_CODES)}")
    return v


def _parse_year(v: int) -> int:
    v = int(v)
    if v < MIN_YEAR:
        raise ValueError(f"Year must be >= {MIN_YEAR}")
    return v


def _parse_geocode(v: int) -> int:
    v = int(v)
    if not (1_000_000 <= v <= 9_999_999):
        raise ValueError(f"Geocode must be a 7-digit integer, got {v}")
    return v


def _parse_export_format(v: str) -> str:
    v = v.lower()
    if v not in _EXPORT_FORMATS:
        raise ValueError(
            f"Invalid format '{v}'. Options: {sorted(_EXPORT_FORMATS)}"
        )
    return v


Disease = Annotated[str, BeforeValidator(_parse_disease)]
UF = Annotated[str, BeforeValidator(_parse_uf)]
Year = Annotated[int, BeforeValidator(_parse_year)]
Geocode = Annotated[int, BeforeValidator(_parse_geocode)]
ExportFormat = Annotated[str, BeforeValidator(_parse_export_format)]
