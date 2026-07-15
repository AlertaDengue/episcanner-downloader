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

### Input requirements

Each row must provide four fields:

| Field | Type | Description |
|-------|------|-------------|
| `SE` or `data_iniSE` or `ew` | `int` (CDC), `datetime`, or `Week` | Epidemiological week. `SE=202401` means week 1 of 2024. |
| `casos_est` | `float` | Estimated number of cases for that week. |
| `geocode` or `municipio_geocodigo` | `int` | 7-digit IBGE municipality code. |
| `p_rt1` | `float` | Probability that Rt > 1 for that week. |

A municipality is analysed only when, **within weeks 45 of year-1 to 35 of year**:
- At least 4 weeks have `p_rt1 > 0.9`, **and**
- Total `casos_est` exceeds 50.

Data can be passed as a `DataFrame`, `list[dict]`, `list[AlertaRow]`, or a single `AlertaRow`:

```python
from episcanner.scanner import EpiScanner
from episcanner.schemas import AlertaRow
from epiweeks import Week

# dicts (database rows)
scanner = EpiScanner([
    {"SE": 202401, "casos_est": 10.0, "geocode": 3550308, "p_rt1": 0.95},
    {"SE": 202402, "casos_est": 25.0, "geocode": 3550308, "p_rt1": 0.92},
], year=2024)

# DataFrame
import pandas as pd
df = pd.DataFrame({
    "SE": [202401, 202402],
    "casos_est": [10.0, 25.0],
    "municipio_geocodigo": [3550308, 3550308],
    "p_rt1": [0.95, 0.92],
})
scanner = EpiScanner(df, year=2024)

# pre-built objects
scanner = EpiScanner([
    AlertaRow(ew=Week(2024, 1), casos_est=10.0, geocode=3550308, p_rt1=0.95),
], year=2024)
```

### Single municipality output

```python
results = scanner.richards()  # list[SirParams]
```

```python
[SirParams(
    geocode=3550308,
    year=2024,
    ep_ini='202402',          # epidemic onset (CDC week)
    ep_pw='202409',           # peak week
    ep_end='202412',          # epidemic end
    ep_dur=10,                # duration in weeks
    peak_week=8.0,            # estimated peak in the time series
    beta=0.789,               # transmission rate
    gamma=0.300,              # recovery rate
    R0=2.63,                  # basic reproduction number
    total_cases=3353.7,       # estimated total cases (asymptote L)
    alpha=0.620,              # Richards shape parameter
    sum_res=0.213,            # mean absolute residual / max cumulative
    t_ini=1,                  # onset position in 52-week window
    t_end=11,                 # end position in 52-week window
)]
```

### Multiple municipalities

Municipalities that fail the transmission threshold are silently skipped:

```python
scanner = EpiScanner([
    {"SE": 202401, "casos_est": 10.0, "geocode": 3550308, "p_rt1": 0.95},  # SP
    {"SE": 202401, "casos_est": 15.0, "geocode": 3304557, "p_rt1": 0.92},  # RJ
    {"SE": 202401, "casos_est":  2.0, "geocode": 5300108, "p_rt1": 0.10},  # low
    # ... 12+ weeks per municipality ...
], year=2024)

results = scanner.richards()
```

```python
[
    SirParams(geocode=3550308, R0=2.63, total_cases=3354.0, ep_pw="202409"),
    SirParams(geocode=3304557, R0=1.85, total_cases=5012.0, ep_pw="202410"),
    # 5300108 skipped — p_rt1 never exceeded 0.9 threshold
]
```

### Export

```python
scanner.richards(export_to="csv",     export_uf="SP")  # SP_2024.csv
scanner.richards(export_to="parquet", export_uf="SP")  # SP_2024.parquet
scanner.richards(export_to="duckdb",  export_uf="SP")  # table SP in episcanner.duckdb
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
