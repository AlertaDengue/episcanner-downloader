from collections import defaultdict
from pathlib import Path

import pandas as pd

from utils import otim, get_SIR_pars


class EpiScanner:
    def __init__(self, last_week: int, data: pd.DataFrame):
        """
        Detecting Epidemic Curves by Scanning Time Series Data

        Parameters
        ----------
        last_week : int
            The last week of data to include in the analysis, represented as
            a two-digit number (e.g., 20 for the 20th week of the year).
        data : pandas.DataFrame
            A pandas DataFrame containing the time series data for all cities.
        """
        self.window = last_week
        self.data = data
        self.results = defaultdict(list)
        self.curves = defaultdict(list)

    def _filter_city(self, geocode):
        dfcity = self.data[self.data.municipio_geocodigo == geocode]
        dfcity.sort_index(inplace=True)
        dfcity["casos_cum"] = dfcity.casos.cumsum()
        return dfcity

    def scan(self, geocode, verbose=True):
        df = self._filter_city(geocode)
        df["year"] = [i.year for i in df.index]
        for y in set(df.year.values):
            if verbose:
                print(f"Scanning year {y}")
            dfy = df[df.year == y]
            has_transmission = dfy.transmissao.sum() > 3
            if not has_transmission:
                if verbose:
                    print(
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
                if verbose:
                    print(
                        f"""
                            R0 in {y}: {
                            self.results[geocode][-1]['sir_pars']['R0']
                        }
                        """
                    )

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

    def to_csv(self, fname_path):
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
        dfpars = pd.DataFrame(data)
        # Create a Path object for the file path
        fname_path = Path(fname_path)
        try:
            # Check if the directory exists and create it if necessary
            fname_path.parent.mkdir(parents=True, exist_ok=True)
            # Write the DataFrame to CSV
            dfpars.to_csv(fname_path)
            print(f"Data exported successfully to {fname_path}")
        except (FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Failed to write CSV file: {e}")
        except Exception as e:
            raise ValueError(f"Unexpected error while writing CSV file: {e}")
