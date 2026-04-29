import pandas as pd

# Intervalle demandé
start = "2023-10-31 23:00:00"
end = "2024-10-29 22:30:00"

# Paramètres
freq = "30min"  # "H" = horaire | "30min" si tu veux demi-heure
prix_constant = 70.0  # €/MWh (modifiable)

# Génération de la timeline
date_range = pd.date_range(start=start, end=end, freq=freq)

# Création du DataFrame
df = pd.DataFrame({
    "datetime": date_range,
    "price_EUR_MWh": prix_constant
})

# Sauvegarde CSV
df.to_csv("../data_manager/data/raw_price/prix_spot_fictif_price_generator_1.csv", index=False)

print(df.head())
print(f"✅ Fichier généré avec {len(df)} lignes")