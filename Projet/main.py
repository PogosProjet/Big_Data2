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
PRICE_CSV   = Path("prix_spot_fictif_price_generator_1.csv")          # chemin vers le fichier prix spots
MONGO_URI   = "mongodb://localhost:27017/"
DB_NAME     = "test"
COLLECTION  = "timeseries"


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
    # horodate stocké en datetime Python par PyMongo → on retire le timezone pour l'alignement
    df["horodate"] = pd.to_datetime(df["horodate"]).dt.tz_localize(None)
    df = df.sort_values("horodate").reset_index(drop=True)
    return df


# ── Contrats / Routers ────────────────────────────────────────────────────────
# Signature commune : fn(conso: DataFrame, prices: DataFrame) -> float
# Ajoutez vos propres contrats ici en suivant ce même pattern.

def contract_spot(conso: pd.DataFrame, prices: pd.DataFrame) -> float:
    """
    Contrat Spot : Σ (valeur_i × prix_spot_i) sur tous les pas de temps communs.
    La jointure est faite sur l'horodatage exact.
    """
    merged = conso.set_index("horodate").join(prices, how="inner")
    return float((merged["valeur"] * merged["price_EUR_MWh"]).sum())


def contract_flat(conso: pd.DataFrame, prices: pd.DataFrame) -> float:
    """
    Contrat Fictif Fixe : Σ (valeur_i × 10).
    Remplacez « 10 » par votre propre formule.
    """
    return float((conso["valeur"] * 10).sum())


# ── Registre des contrats ─────────────────────────────────────────────────────
# Pour ajouter un contrat : (nom_affichage, fonction)
CONTRACTS: list[tuple[str, callable]] = [
    ("Spot Marché",            contract_spot),
    ("Tarif Fixe (10 €/MWh)", contract_flat),
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
            total = round(fn(conso, prices), 2)
        except Exception as e:
            total = None
        results.append({"contract": name, "price": total})

    # Tri croissant (None en dernier)
    results.sort(key=lambda x: (x["price"] is None, x["price"] or 0))

    return {
        "id":       request.id,
        "n_points": len(conso),
        "results":  results,
    }


# ── Servir le frontend ────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")