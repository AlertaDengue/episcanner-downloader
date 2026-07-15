__all__ = ["EpiScanner"]

from pathlib import Path

import duckdb
from duckdb import BinderException, CatalogException
from loguru import logger
import numpy as np
import pandas as pd
from pydantic import TypeAdapter

from .models import Richards
from .schemas import AlertaData, SirParams, parse_alerta
from .types import UF, ExportFormat, Year

CACHEPATH = Path.home() / "episcanner"


class EpiScanner:
    def __init__(
        self,
        data: AlertaData,
        year: Year,
    ):
        self.data = parse_alerta(data)
        self.year = TypeAdapter(Year).validate_python(year)

    def richards(
        self,
        export_to: ExportFormat | None = None,
        export_uf: UF | None = None,
        export_output: str | Path = CACHEPATH,
    ) -> list[SirParams]:
        models, curves = Richards.scan(self.data, self.year)
        results = []
        for geocode, model in models.items():
            curve = curves[geocode]
            sir = model.get_SIR_pars()
            ep = model.comp_duration(curve)
            residuals = np.array(curve.richards) - np.array(curve.casos_cum)
            sum_res = float(sum(abs(residuals)) / max(curve.casos_cum))

            results.append(
                SirParams(
                    geocode=geocode,
                    year=self.year,
                    ep_ini=ep.ini,
                    ep_pw=ep.pw,
                    ep_end=ep.end,
                    ep_dur=ep.dur,
                    peak_week=model.tp1,
                    beta=sir.beta,
                    gamma=sir.gamma,
                    R0=sir.R0,
                    total_cases=model.L,
                    alpha=model.a,
                    sum_res=sum_res,
                    t_ini=ep.t_ini,
                    t_end=ep.t_end,
                )
            )

        if export_to is not None and export_uf is not None:
            if export_to not in ("csv", "parquet", "duckdb"):
                raise ValueError(
                    "Invalid format "
                    f"'{export_to}'. Options: csv, parquet, duckdb"
                )
            self._export(results, export_to, export_uf, export_output)

        return results

    def _export(
        self,
        results: list[SirParams],
        to: ExportFormat,
        uf: str,
        output_dir: str | Path = CACHEPATH,
    ) -> str:
        if not results:
            raise ValueError("No data to export")

        df = pd.DataFrame([r.model_dump() for r in results])

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        file = output_dir / f"{uf}_{self.year}.{to}"

        if file.exists() and to != "duckdb":
            logger.warning(f"Overriding {file}")
            file.unlink()

        try:
            if to == "csv":
                df.to_csv(file, index=False)
            elif to == "parquet":
                df.to_parquet(file, index=False)
            elif to == "duckdb":
                file = self._to_duckdb(df, uf, output_dir)

            logger.info(f"{uf} data for {self.year} wrote to {file}")
        except (FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Failed to write file: {e}")
        except Exception as e:
            raise ValueError(f"Unexpected error while writing file: {e}")

        return str(file.absolute())

    def _to_duckdb(
        self, df: pd.DataFrame, uf: str, output_dir: str | Path
    ) -> Path:
        db = Path(output_dir) / "episcanner.duckdb"
        con = duckdb.connect(str(db.absolute()))

        try:
            str_cols = df.select_dtypes(include=["object"]).columns
            df[str_cols] = df[str_cols].astype("string")

            con.register("data", df)

            try:
                result = con.execute(
                    f"SELECT COUNT(*) FROM '{uf}'" f" WHERE year = {self.year}"
                ).fetchone()
                rows = result[0] if result else 0

                if rows > 0:
                    logger.warning(f"Overriding data for {self.year}")
                    con.execute(f"DELETE FROM '{uf}' WHERE year = {self.year}")
                con.execute(f"INSERT INTO '{uf}' SELECT * FROM data")
            except (CatalogException, BinderException):
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS '{uf}'"
                    " AS SELECT * FROM data"
                )
        finally:
            con.unregister("data")
            con.close()
        return db
