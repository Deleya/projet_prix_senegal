from pathlib import Path
from datetime import datetime

import pandas as pd
from project_paths import RAW_DATA_DIR, ensure_data_directories, find_latest_file

SOURCES = {
    "Auchan": "donnees_brutes_auchan_*.csv",
    "Sakanal": "donnees_brutes_sakanal_*.csv",
    "Diarle": "donnees_brutes_diarle_*.csv",
}

COLONNES_ATTENDUES = [
    "nom_produit",
    "categorie",
    "prix",
    "magasin",
    "date_scraping",
    "url_produit",
]


def trouver_dernier_fichier(pattern):
    return find_latest_file(RAW_DATA_DIR, pattern)


def charger_source(nom_magasin, pattern):
    fichier = trouver_dernier_fichier(pattern)
    if fichier is None:
        print(f"Aucun fichier trouve pour {nom_magasin} avec le motif {pattern}")
        return None

    df = pd.read_csv(fichier)
    print(f"{nom_magasin} charge : {len(df)} lignes depuis {fichier.name}")

    colonnes_manquantes = [col for col in COLONNES_ATTENDUES if col not in df.columns]
    if colonnes_manquantes:
        print(
            f"Fichier ignore pour {nom_magasin} : colonnes manquantes {colonnes_manquantes}"
        )
        return None

    return df[COLONNES_ATTENDUES].copy()


def fusionner_donnees():
    ensure_data_directories()
    date_today = datetime.now().strftime("%Y-%m-%d")
    dataframes = []

    for nom_magasin, pattern in SOURCES.items():
        df = charger_source(nom_magasin, pattern)
        if df is not None:
            dataframes.append(df)

    if not dataframes:
        print("Aucune source exploitable trouvee. Fusion annulee.")
        return

    df_combine = pd.concat(dataframes, ignore_index=True)
    df_combine = df_combine.drop_duplicates(
        subset=["nom_produit", "prix", "magasin"],
        keep="first",
    )
    df_combine = df_combine.sort_values(
        by=["magasin", "categorie", "nom_produit"],
        kind="stable",
    )
    df_combine = df_combine[COLONNES_ATTENDUES]

    filename = RAW_DATA_DIR / f"donnees_brutes_combinees_{date_today}.csv"
    df_combine.to_csv(filename, index=False, encoding="utf-8-sig")

    print("\nFUSION TERMINEE AVEC SUCCES")
    print(f"Total lignes uniques : {len(df_combine)}")
    print(f"Fichier cree : {filename.name}")
    print("\nRepartition par magasin :")
    print(df_combine["magasin"].value_counts())
    print("\nApercu des 5 premieres lignes :")
    print(df_combine.head())


if __name__ == "__main__":
    fusionner_donnees()
