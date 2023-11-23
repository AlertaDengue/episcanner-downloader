from collections import defaultdict
from pathlib import Path

import lmfit as lm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from epiweeks import Week
from lmfit import Parameters


# Richards Model
@np.vectorize
def richards(L, a, b, t, tj):
    j = L - L * (1 + a * np.exp(b * (t - tj))) ** (-1 / a)
    return j


def obj_fun(params, t_ini, t_fin, df):
    """Objective function"""
    window = (t_fin - t_ini,)
    pars = params.valuesdict()
    L = pars["L1"]
    tp = pars["tp1"]
    a = pars["a1"]
    b = pars["b1"]

    t_range = np.arange(t_fin - t_ini)
    richfun = richards(L, a, b, t_range, tp)
    serie = df.loc[t_ini:t_fin].casos_cum.values

    mse = (serie - richfun) ** 2 / window

    return mse


def get_SIR_pars(rp: dict):
    """
    Returns the SIR parameters based on the Richards model's parameters (rp)
    """
    a = rp["a1"]
    b = rp["b1"]
    tc = rp["tp1"]
    pars = {
        "beta": b / a,
        "gamma": (b / a) - b,
        "R0": (b / a) / ((b / a) - b),
        "tc": tc,
    }
    return pars


def otim(df, t_ini, t_fin, verbose=False):
    df.reset_index(inplace=True)
    df["casos_cum"] = df.casos.cumsum()
    params = Parameters()
    params.add("gamma", min=0.3, max=0.33)
    # params.add("gamma", min=0.95, max=1.05)
    params.add("L1", min=1.0, max=5e5)
    params.add("tp1", min=5, max=35)
    params.add("b1", min=1e-6, max=1)
    params.add("a1", expr="b1/(gamma + b1)", min=0.001, max=1)

    window = min(int(t_fin - t_ini), len(df))
    t_range = np.arange(window)

    out = lm.minimize(
        obj_fun, params, args=(0, window, df), method="diferential_evolution"
    )
    if verbose:
        if out.success:
            print(f"found  match after {out.nfev} tries")
        else:
            print("No match found")
            return False, df

    pars = out.params
    pars = pars.valuesdict()

    # serie = df.loc[t_ini:t_fin].casos_cum.values
    richfun_opt = richards(
        pars["L1"], pars["a1"], pars["b1"], t_range, pars["tp1"]
    )

    df = df.iloc[:window]

    df["richards"] = richfun_opt + np.zeros(window)

    return out, df


def comp_duration(curve):
    """
    This function computes an estimation of the epidemic beginning,
    duration and end based of the peak of richards model estimated;
    """

    df_aux = pd.DataFrame()

    df_aux["dates"] = curve.iloc[:52].data_iniSE
    # print(curve.columns)
    df_aux["SE"] = [Week.fromdate(i).cdcformat() for i in df_aux["dates"]]
    # df_aux['richards'] = curve.richards
    df_aux["diff_richards"] = np.concatenate(
        ([0], np.diff(curve.richards)), axis=0
    )

    max_c = df_aux["diff_richards"].max()

    df_aux = df_aux.loc[df_aux.diff_richards >= (0.05) * max_c].sort_index()

    ini = str(df_aux["SE"].values[0])

    end = str(df_aux["SE"].values[-1])

    dur = int(end[-2:]) - int(ini[-2:])

    ep_dur = {"ini": ini, "end": end, "dur": dur}

    return ep_dur


class EpiScanner:
    def __init__(self, last_week: int, data: pd.DataFrame, muninames):
        """
        Detecting Epidemic Curves by Scanning Time Series Data

        Parameters
        ----------
        last_week : int
            The last week of data to include in the analysis, represented as
            a two-digit number (e.g., 20 for the 20th week of the year).
        data : pandas.DataFrame
            A pandas DataFrame containing the time series data for all cities.
        muninames: dict
            A dictionary with the muninames and codes
        """
        self.window = last_week
        self.data = data
        self.results = defaultdict(list)
        self.curves = defaultdict(list)
        self.muninames = muninames

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
                dfy[["casos", "casos_cum"]].iloc[0 : self.window],  # NOQA E203
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
        self.curves[geocode].append(
            {
                "year": year,
                "df": curve,
                #  add residuals and comp duration
                "residuals": abs(curve.richards - curve.casos_cum),
                "sum_res": sum(abs(curve.richards - curve.casos_cum))
                / max(curve.casos_cum),
                "ep_time": comp_duration(curve),
            }
        )

    def get_residuals(self, geocode, year):
        """
        Get residuals for the years fitted curve from
        returns Dataframe if thereis a fit for that year, otherwise None
        """
        df = None
        for curve in self.curves[geocode]:
            if year != curve["year"]:
                continue
            else:
                df = curve["df"]
                df["residuals"] = df.richards - df.casos_cum
                return df

    def plot_fit(self, geocode, year=0):
        if year == 0:
            nyears = len(self.curves[geocode])
            nrows = nyears // 2 if nyears % 2 == 0 else nyears // 2 + 1
            ncols = 2
        else:
            ncols = 1
            nrows = 1
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(10, 10))
        axes = axes.ravel()
        i = 0
        for curve in self.curves[geocode]:
            y = curve["year"]
            df = curve["df"]
            df.set_index("data_iniSE", inplace=True)
            df["residuals"] = curve["residuals"]

            if not df.index.name == "data_iniSE":
                df.set_index("data_iniSE", inplace=True)
            if year != 0 and y != year:
                continue

            df.casos_cum.plot.area(
                ax=axes[i], alpha=0.3, color="r", label=f"data_{y}", rot=45
            )
            df.richards.plot(ax=axes[i], label="model", use_index=True)

            df.residuals.plot(ax=axes[i], marker="+", label="Abs. residuals")

            ep_duration = curve["ep_time"]

            axes[i].axvline(
                df.loc[
                    df.index
                    == pd.to_datetime(
                        Week.fromstring(ep_duration["ini"]).startdate()
                    )
                ].index.values[0],
                color="black",
                ls="--",
            )

            axes[i].axvline(
                df.loc[
                    df.index
                    == pd.to_datetime(
                        Week.fromstring(ep_duration["end"]).startdate()
                    )
                ].index.values[0],
                color="black",
                ls="--",
                label="Epidemic \n period",
            )

            axes[i].set_title(geocode)

            axes[i].legend()
            i += 1
        plt.tight_layout()

    def to_csv(self, fname_path):
        data = {
            "geocode": [],
            "muni_name": [],
            "year": [],
            "peak_week": [],
            "beta": [],
            "gamma": [],
            "R0": [],
            "total_cases": [],
            "alpha": [],
            "sum_res": [],
            "ep_ini": [],
            "ep_end": [],
            "ep_dur": [],
        }
        for gc, curve in self.curves.items():
            for c in curve:
                data["geocode"].append(gc)
                data["muni_name"].append(self.muninames[gc])
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

                # new columns
                data["sum_res"].append(c["sum_res"])
                ep_duration = c["ep_time"]
                data["ep_ini"].append(ep_duration["ini"])
                data["ep_end"].append(ep_duration["end"])
                data["ep_dur"].append(ep_duration["dur"])

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
