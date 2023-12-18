import os
import pathlib

import lmfit as lm
import numpy as np
from lmfit import Parameters

CACHEPATH = os.getenv(
    "EPISCANNER_CACHEPATH",
    os.path.join(str(pathlib.Path.home()), "episcanner"),
)

__cachepath__ = pathlib.Path(CACHEPATH)
__cachepath__.mkdir(exist_ok=True)

STATES = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AM": "Amazonas",
    "AP": "Amapá",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MG": "Minas Gerais",
    "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso",
    "PA": "Pará",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "PR": "Paraná",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RO": "Rondônia",
    "RR": "Roraima",
    "RS": "Rio Grande do Sul",
    "SC": "Santa Catarina",
    "SE": "Sergipe",
    "SP": "São Paulo",
    "TO": "Tocantins",
}


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
    params.add("gamma", min=0.95, max=1.05)
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

    df = df.copy()
    df.loc[:, "richards"] = richfun_opt + np.zeros(window)

    return out, df