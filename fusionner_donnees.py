import pandas as pd
from datetime import datetime

def fusionner_donnees():
    date_today = datetime.now().strftime("%Y-%m-%d")
    
    # === Charge les deux fichiers (change les noms si tes dates sont différentes) ===
    try:
        df_sakanal = pd.read_csv(f"donnees_brutes_sakanal_{date_today}.csv")
        print(f"✅ Sakanal chargé : {len(df_sakanal)} lignes")
    except FileNotFoundError:
        print("⚠️ Fichier Sakanal non trouvé → je cherche avec la date d'hier ou sans date")
        df_sakanal = pd.read_csv("donnees_brutes_sakanal_2026-04-14.csv")  # ajuste si besoin
    
    try:
        df_auchan = pd.read_csv(f"donnees_brutes_auchan_{date_today}.csv")
        print(f"✅ Auchan chargé : {len(df_auchan)} lignes")
    except FileNotFoundError:
        df_auchan = pd.read_csv("donnees_brutes_auchan_2026-04-15.csv")  # ajuste si besoin
    
    # === Fusion ===
    df_combine = pd.concat([df_sakanal, df_auchan], ignore_index=True)
    
    # === Nettoyage final (standardisation) ===
    df_combine = df_combine.drop_duplicates(subset=["nom_produit", "prix", "magasin"], keep='first')
    df_combine = df_combine.sort_values(by=["magasin", "categorie", "nom_produit"])
    
    # Réorganisation des colonnes (propre)
    colonnes = ["nom_produit", "categorie", "prix", "magasin", "date_scraping", "url_produit"]
    df_combine = df_combine[colonnes]
    
    # Sauvegarde
    filename = f"donnees_brutes_combinees_{date_today}.csv"
    df_combine.to_csv(filename, index=False, encoding="utf-8-sig")
    
    print(f"\n🎉 FUSION TERMINÉE AVEC SUCCÈS !")
    print(f"   Total lignes uniques : {len(df_combine)}")
    print(f"   Fichier créé : {filename}")
    print("\nRépartition par magasin :")
    print(df_combine["magasin"].value_counts())
    print("\nAperçu des 5 premières lignes :")
    print(df_combine.head())

if __name__ == "__main__":
    fusionner_donnees()