import pandas as pd
import hashlib
from pathlib import Path
from pymongo import MongoClient

def make_safe_id(old_id, filename):
    parts = filename.replace("-", "_").split("_")
    suffix = parts[1] + parts[2]
    raw = f"{filename}::{old_id}"
    h = hashlib.sha256(raw.encode()).hexdigest()[:12]
    return f"{h}_{suffix}"

folder = Path("../data/raw_conso")

dfs = []

for f in folder.glob("*.csv"):
    print(f"Lecture de {f}")

    df = pd.read_csv(f, sep=";")

    # Normalisation des colonnes
    df.columns = df.columns.str.strip().str.lower()

    if "horodate" not in df.columns:
        exit("Error pas de colonne horodate")
        continue

    df["horodate"] = (
        pd.to_datetime(df["horodate"], utc=True, errors="coerce")
        .dt.strftime("%Y-%m-%dT%H:%M:%S")
    )

    id_map = {
        old_id: make_safe_id(old_id, f.stem)
        for old_id in df["id"].unique()
    }
    df["id"] = df["id"].map(id_map)

    dfs.append(df)

# Concaténation finale
big_df = pd.concat(dfs, ignore_index=True)

print(big_df.shape)
print(big_df.head())


def insert_timeseries(df, collection_name):

    # Connexion MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["test"]

    # Création de la collection Time Series si elle n'existe pas
    if collection_name not in db.list_collection_names():
        db.create_collection(
            collection_name,
            timeseries={
                "timeField": "horodate",
                "metaField": "metadata",
                "granularity": "minutes"
            }
        )

    collection = db[collection_name]

    df["horodate"] = pd.to_datetime(df["horodate"])

    # Formatage et insertion
    documents = [
        {
            "horodate": row["horodate"],
            "metadata": {"id": row["id"]},
            "valeur": row["valeur"]
        }
        for _, row in df.iterrows()
    ]

    if documents:
        result = collection.insert_many(documents)
        print(f"[{collection_name}] {len(result.inserted_ids)} documents insérés")
    else:
        print(f"[{collection_name}] Aucun document à insérer")


insert_timeseries(big_df, "timeseries")