"""
Energie Pricer — Backend FastAPI
---------------------------------
Lancer : uvicorn main:app --reload
Dépendances : fastapi uvicorn pymongo pandas
"""

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pymongo import MongoClient
import pandas as pd
from pathlib import Path
from functools import lru_cache

app = FastAPI(title="Energie Pricer API")

# ── Configuration ─────────────────────────────────────────────────────────────
PRICE_CSV  = Path("prix_spot_fictif_price_generator_1.csv")
MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "test"
COLLECTION = "timeseries"

# Heures Pleines : lundi–vendredi, 8h–20h
HP_DAYS  = range(0, 5)
HP_START = 8
HP_END   = 20


# ── Helper NaN-safe ───────────────────────────────────────────────────────────
def safe_float(val, ndigits: int = 4):
    """Convertit en float arrondi ; retourne None si NaN ou Inf."""
    try:
        f = float(val)
        if not (f == f) or f in (float("inf"), float("-inf")):
            return None
        return round(f, ndigits)
    except (TypeError, ValueError):
        return None


# ── Prix spots (chargé une seule fois) ────────────────────────────────────────
@lru_cache(maxsize=1)
def load_prices() -> pd.DataFrame:
    df = pd.read_csv(PRICE_CSV)
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)
    df = df.set_index("datetime").sort_index()
    return df


# ── Lecture MongoDB ────────────────────────────────────────────────────────────
def get_timeseries(ts_id: str) -> pd.DataFrame:
    client = MongoClient(MONGO_URI)
    docs = list(
        client[DB_NAME][COLLECTION].find(
            {"metadata.id": ts_id},
            {"_id": 0, "horodate": 1, "valeur": 1},
        )
    )
    if not docs:
        raise HTTPException(status_code=404, detail=f"ID '{ts_id}' introuvable en base.")

    df = pd.DataFrame(docs)
    df["horodate"] = pd.to_datetime(df["horodate"]).dt.tz_localize(None)
    df = df.sort_values("horodate").reset_index(drop=True)
    return df


# ── Helpers ───────────────────────────────────────────────────────────────────
def _merge(conso: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    return conso.set_index("horodate").join(prices, how="inner")


def _is_hp(idx: pd.DatetimeIndex) -> pd.Series:
    return pd.Series(
        (idx.dayofweek.isin(HP_DAYS)) & (idx.hour >= HP_START) & (idx.hour < HP_END),
        index=idx,
    )


# ── Contrats ──────────────────────────────────────────────────────────────────

def contract_spot(conso: pd.DataFrame, prices: pd.DataFrame) -> dict:
    merged = _merge(conso, prices)
    total_volume = merged["valeur"].sum()
    total = float((merged["valeur"] * merged["price_EUR_MWh"]).sum())
    prix_moyen = round(total / total_volume, 4) if total_volume else 0.0
    return {"total": total, "prix_moyen_mwh": prix_moyen}


def contract_fixed_weighted(conso: pd.DataFrame, prices: pd.DataFrame) -> dict:
    merged = _merge(conso, prices)
    total_volume = merged["valeur"].sum()
    if total_volume == 0:
        return {"total": 0.0, "prix_moyen_mwh": 0.0}
    prix_fixe = float((merged["valeur"] * merged["price_EUR_MWh"]).sum() / total_volume)
    total = prix_fixe * total_volume
    return {"total": float(total), "prix_moyen_mwh": round(prix_fixe, 4)}


def contract_quarterly_weighted(conso: pd.DataFrame, prices: pd.DataFrame) -> dict:
    merged = _merge(conso, prices)
    merged["quarter"] = merged.index.to_period("Q")

    total        = 0.0
    total_volume = 0.0
    quarters     = []

    for period, grp in merged.groupby("quarter"):
        vol = grp["valeur"].sum()
        if vol == 0:
            continue
        prix_q = float((grp["valeur"] * grp["price_EUR_MWh"]).sum() / vol)
        cost_q = prix_q * vol
        total        += cost_q
        total_volume += vol
        quarters.append({
            "label":      str(period),
            "prix_mwh":   round(prix_q, 4),
            "volume_mwh": round(float(vol), 4),
            "cout_eur":   round(float(cost_q), 2),
        })

    prix_moyen = round(total / total_volume, 4) if total_volume else 0.0
    return {
        "total":          float(total),
        "prix_moyen_mwh": prix_moyen,
        "quarters":       quarters,
    }


def contract_peak_offpeak(conso: pd.DataFrame, prices: pd.DataFrame) -> dict:
    merged  = _merge(conso, prices)
    hp_mask = _is_hp(merged.index)

    result       = {}
    total        = 0.0
    total_volume = 0.0

    for mask, key in [(hp_mask, "hp"), (~hp_mask, "hc")]:
        grp = merged[mask]
        vol = grp["valeur"].sum()
        if vol == 0:
            result[f"prix_{key}_mwh"]   = 0.0
            result[f"volume_{key}_mwh"] = 0.0
            result[f"cout_{key}_eur"]   = 0.0
            continue
        prix = float((grp["valeur"] * grp["price_EUR_MWh"]).sum() / vol)
        cost = prix * vol
        result[f"prix_{key}_mwh"]   = round(prix, 4)
        result[f"volume_{key}_mwh"] = round(float(vol), 4)
        result[f"cout_{key}_eur"]   = round(float(cost), 2)
        total        += cost
        total_volume += vol

    prix_moyen = round(total / total_volume, 4) if total_volume else 0.0
    result["total"]          = float(total)
    result["prix_moyen_mwh"] = prix_moyen
    return result


# ── Statistiques marché ───────────────────────────────────────────────────────

def compute_market_stats(conso: pd.DataFrame, prices: pd.DataFrame) -> dict:
    merged = _merge(conso, prices)
    p = merged["price_EUR_MWh"]
    v = merged["valeur"]

    # Stats annuelles
    annual = {
        "mean":          safe_float(p.mean()),
        "min":           safe_float(p.min()),
        "max":           safe_float(p.max()),
        "weighted_mean": safe_float((v * p).sum() / v.sum()) if v.sum() else 0.0,
    }

    # Stats par trimestre
    merged_q = merged.copy()
    merged_q["quarter"] = merged_q.index.to_period("Q")
    quarterly = []
    for period, grp in merged_q.groupby("quarter"):
        vol_q = grp["valeur"].sum()
        quarterly.append({
            "label":         str(period),
            "mean":          safe_float(grp["price_EUR_MWh"].mean()),
            "weighted_mean": safe_float(
                (grp["valeur"] * grp["price_EUR_MWh"]).sum() / vol_q
            ) if vol_q else 0.0,
        })

    # Stats HP / HC
    hp_mask   = _is_hp(merged.index)
    hp_prices = merged.loc[hp_mask,  "price_EUR_MWh"]
    hc_prices = merged.loc[~hp_mask, "price_EUR_MWh"]
    peak = {
        "mean_hp": safe_float(hp_prices.mean()) if len(hp_prices) else 0.0,
        "mean_hc": safe_float(hc_prices.mean()) if len(hc_prices) else 0.0,
    }

    # Série spot journalière pour graphique annuel
    spot_chart = [
        {"t": str(ts.date()), "v": safe_float(val)}
        for ts, val in merged["price_EUR_MWh"].resample("D").mean().dropna().items()
    ]

    # Série conso journalière pour graphique annuel
    conso_chart = [
        {"t": str(ts.date()), "v": safe_float(val)}
        for ts, val in merged["valeur"].resample("D").sum().dropna().items()
    ]

    # Série HP/HC sur une semaine représentative (première semaine lun→dim)
    mondays = merged[merged.index.dayofweek == 0]
    hphc_chart = []
    if len(mondays):
        week_start = mondays.index[0]
        week_end   = week_start + pd.Timedelta(days=7)
        week_data  = merged.loc[week_start:week_end]
        hp_mask_w  = _is_hp(week_data.index)
        hphc_chart = [
            {
                "t":    ts.strftime("%Y-%m-%dT%H:%M"),
                "v":    safe_float(row["valeur"]),
                "hp":   bool(hp_mask_w.loc[ts]),
                "spot": safe_float(row["price_EUR_MWh"]),
            }
            for ts, row in week_data.iterrows()
        ]

    return {
        "annual":      annual,
        "quarterly":   quarterly,
        "peak":        peak,
        "spot_chart":  spot_chart,
        "conso_chart": conso_chart,
        "hphc_chart":  hphc_chart,
    }


# ── Registre des contrats ─────────────────────────────────────────────────────
CONTRACTS: list[tuple[str, callable]] = [
    ("Spot Marché",           contract_spot),
    ("Prix Fixe Annuel",      contract_fixed_weighted),
    ("Prix Trimestriel",      contract_quarterly_weighted),
    ("Peak / Off-Peak (PME)", contract_peak_offpeak),
]


# ── Endpoint principal ────────────────────────────────────────────────────────
class AnalyzeRequest(BaseModel):
    id: str


@app.post("/analyze")
async def analyze(request: AnalyzeRequest):
    conso  = get_timeseries(request.id)
    prices = load_prices()

    results = []
    for name, fn in CONTRACTS:
        try:
            res = fn(conso, prices)
            res["contract"] = name
            res["price"]    = round(res.get("prix_moyen_mwh") or 0, 4)
        except Exception:
            res = {"contract": name, "price": None, "prix_moyen_mwh": None}
        results.append(res)

    results.sort(key=lambda x: (x["price"] is None, x["price"] or 0))

    stats = compute_market_stats(conso, prices)

    return {
        "id":       request.id,
        "n_points": len(conso),
        "results":  results,
        "stats":    stats,
    }


# ── Servir le frontend ────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")