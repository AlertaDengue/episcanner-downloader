# Episcanner

Epidemic curve detection by scanning time series data.

## Installation

```shell
git clone https://github.com/AlertaDengue/episcanner-downloader.git
cd episcanner-downloader
pip install poetry && poetry install
```

## Pipeline

```
AlertaRow ──► Richards.scan() ──► EpiScanner.richards() ──► SirParams ──► export
  (data)        (fit per city)      (build output)          (results)     (csv/parquet/duckdb)
```

## Usage

`EpiScanner` accepts any of these as `data`:

```python
from episcanner.scanner import EpiScanner

# from a list of dicts (e.g. database rows)
scanner = EpiScanner([
    {"SE": 202401, "casos_est": 10.0, "geocode": 3550308, "p_rt1": 0.95},
    {"SE": 202402, "casos_est": 25.0, "geocode": 3550308, "p_rt1": 0.92},
], year=2024)

# from a pandas DataFrame
import pandas as pd
df = pd.DataFrame({"SE": [202401, 202402], "casos_est": [10.0, 25.0], ...})
scanner = EpiScanner(df, year=2024)

# from pre-built AlertaRow objects
from episcanner.schemas import AlertaRow
from epiweeks import Week
scanner = EpiScanner([
    AlertaRow(ew=Week(2024, 1), casos_est=10.0, geocode=3550308, p_rt1=0.95),
], year=2024)

results = scanner.richards()            # list[SirParams]
scanner.richards(export_to="csv", export_uf="SP")       # writes SP_2024.csv
scanner.richards(export_to="parquet", export_uf="SP")   # writes SP_2024.parquet
scanner.richards(export_to="duckdb", export_uf="SP")    # writes episcanner.duckdb table SP
```

The `parse_alerta` helper converts any format into `list[AlertaRow]`:

```python
from episcanner.schemas import parse_alerta

rows = parse_alerta(df)                 # DataFrame → list[AlertaRow]
rows = parse_alerta([{"SE": 202401, ...}])  # dicts → list[AlertaRow]
```

## Standalone Richards model

```python
from epiweeks import Week
from episcanner.models import Richards
from episcanner.schemas import AlertRow

data = [AlertRow(ew=Week(2024, w), casos_est=c) for w, c in ...]
model = Richards.fit(data)                     # optimize parameters

curve = model.to_curve(data)                   # FittedCurve
sir   = model.get_SIR_pars()                   # SIRPars(beta, gamma, R0, tc)
ep    = model.comp_duration(curve)             # EpDuration(ini, pw, end, dur)

y = model.evaluate(np.arange(52))              # predicted values at t
```

Or instantiate with known parameters:

```python
model = Richards(L=3354.0, a=0.62, b=0.49, tp1=8.0, gamma=0.3)
```

## Standalone functions

```python
from episcanner.analysis.richards import equation, get_SIR_pars, comp_duration
from episcanner.schemas import RichardsPars, FittedCurve

equation(L=100.0, a=0.5, b=0.3, t=np.array([0,5,10]), tj=5.0)
get_SIR_pars(RichardsPars(gamma=0.3, L1=100.0, tp1=5.0, b1=0.3, a1=0.5))
comp_duration(curve, tp1=8.0)
```

## Schemas

| Schema | Fields |
|--------|--------|
| `AlertaRow` | `ew: Week`, `casos_est`, `geocode`, `p_rt1` |
| `AlertRow` | `ew: Week`, `casos_est` (fitting input) |
| `FittedCurve` | `ew`, `casos_cum`, `richards` |
| `RichardsPars` | `gamma`, `L1`, `tp1`, `b1`, `a1` |
| `SIRPars` | `beta`, `gamma`, `R0`, `tc` |
| `EpDuration` | `ini`, `pw`, `end`, `dur`, `t_ini`, `t_end` |
| `SirParams` | `geocode`, `year`, `ep_*`, `peak_week`, `beta`, `gamma`, `R0`, `total_cases`, `alpha`, `sum_res` |

## Types

Validated type annotations with normalization:

| Type | Validates |
|------|-----------|
| `Disease` | dengue, zika, chik (chikungunya→chik, lowercase) |
| `UF` | 27 Brazilian state codes (uppercase) |
| `Year` | ≥ 2011 |
| `Geocode` | 7-digit integer |
| `ExportFormat` | csv, parquet, duckdb, schema (lowercase) |

## Modules

```
episcanner/
├── types.py          # Disease, UF, Year, Geocode, ExportFormat, CID10
├── schemas.py        # AlertaRow, AlertRow, FittedCurve, RichardsPars, SIRPars, EpDuration, SirParams
├── models.py         # AnalysisModel (ABC), Richards
├── scanner.py        # EpiScanner
└── analysis/
    ├── richards.py   # equation, objective, get_SIR_pars, comp_duration (standalone)
    └── __init__.py
```

## License

MIT
