import pandas as pd
from project_paths import KPI_DATA_DIR, PROCESSED_DATA_DIR, ensure_data_directories

COLONNES_REQUISES = [
    "nom_nettoye",
    "categorie_standardisee",
    "magasin_standardise",
    "prix",
    "prix_par_kg_ou_l",
    "prix_par_piece",
    "cle_matching_exacte",
    "date_scraping",
    "jour",
]

STATUTS_COMPARABLES = {"comparable_normalise"}


def find_analytic_files():
    return sorted(
        PROCESSED_DATA_DIR.glob("donnees_analytiques_kpi_*.csv"),
        key=lambda p: p.stat().st_mtime,
    )


def load_analytics_history():
    files = find_analytic_files()
    if not files:
        raise FileNotFoundError("Aucun fichier donnees_analytiques_kpi_*.csv trouve.")

    frames = []
    for path in files:
        df = pd.read_csv(path)
        missing = [col for col in COLONNES_REQUISES if col not in df.columns]
        if missing:
            print(f"Fichier ignore {path.name} : colonnes manquantes {missing}")
            continue
        df["source_file"] = path.name
        frames.append(df)

    if not frames:
        raise ValueError("Aucun fichier analytique exploitable trouve.")

    history = pd.concat(frames, ignore_index=True)
    history["date_scraping"] = pd.to_datetime(history["date_scraping"], errors="coerce")
    history["jour"] = pd.to_datetime(history["jour"], errors="coerce")
    history["prix"] = pd.to_numeric(history["prix"], errors="coerce")
    history["prix_par_kg_ou_l"] = pd.to_numeric(history["prix_par_kg_ou_l"], errors="coerce")
    history["prix_par_piece"] = pd.to_numeric(history["prix_par_piece"], errors="coerce")
    if "score_matching" in history.columns:
        history["score_matching"] = pd.to_numeric(history["score_matching"], errors="coerce")
    history = history.dropna(subset=["date_scraping", "jour", "prix", "magasin_standardise", "cle_matching_exacte"]).copy()
    history = history.loc[history["prix"] > 0].copy()
    if "statut_comparabilite" not in history.columns:
        history["statut_comparabilite"] = "a_revoir"
    if "motif_comparabilite" not in history.columns:
        history["motif_comparabilite"] = ""
    if "type_prix_reference" not in history.columns:
        history["type_prix_reference"] = "raw_price"
    if "score_matching" not in history.columns:
        history["score_matching"] = None
    history = history.drop_duplicates(
        subset=["magasin_standardise", "jour", "cle_matching_exacte", "prix", "source_file"],
        keep="last",
    )
    history["mois"] = history["jour"].dt.strftime("%Y-%m")
    history["annee"] = history["jour"].dt.year
    history["mois_num"] = history["jour"].dt.month
    return history


def choose_comparable_price(row):
    if pd.notna(row.get("prix_par_kg_ou_l")):
        return row["prix_par_kg_ou_l"], "unit_kg_l"
    if pd.notna(row.get("prix_par_piece")):
        return row["prix_par_piece"], "unit_piece"
    return row["prix"], "raw_price"


def enrich_price_metric(df):
    metrics = df.apply(choose_comparable_price, axis=1, result_type="expand")
    metrics.columns = ["prix_comparable", "type_prix_comparable"]
    df = pd.concat([df, metrics], axis=1)
    df["prix_comparable"] = pd.to_numeric(df["prix_comparable"], errors="coerce")
    return df.dropna(subset=["prix_comparable"]).copy()


def filter_reliable_history(df):
    filtered = df.loc[df["statut_comparabilite"].isin(STATUTS_COMPARABLES)].copy()
    if filtered.empty:
        return filtered
    return filtered


def build_daily_reference_panel(df):
    panel = (
        df.groupby(
            ["jour", "mois", "categorie_standardisee", "cle_matching_exacte", "magasin_standardise"],
            dropna=False,
        )
        .agg(
            nom_reference=("nom_nettoye", "first"),
            prix_obs=("prix_comparable", "mean"),
            type_prix=("type_prix_comparable", "first"),
            score_matching_moyen=("score_matching", "mean"),
            statut_comparabilite=("statut_comparabilite", "first"),
        )
        .reset_index()
    )

    coverage = (
        panel.groupby(["jour", "cle_matching_exacte"])["magasin_standardise"]
        .nunique()
        .rename("nb_magasins_jour")
        .reset_index()
    )
    panel = panel.merge(coverage, on=["jour", "cle_matching_exacte"], how="left")
    type_coverage = (
        panel.groupby(["jour", "cle_matching_exacte"])["type_prix"]
        .nunique()
        .rename("nb_types_prix_jour")
        .reset_index()
    )
    panel = panel.merge(type_coverage, on=["jour", "cle_matching_exacte"], how="left")
    panel["comparable_multi_store"] = panel["nb_magasins_jour"] >= 2
    panel["comparabilite_stricte"] = panel["comparable_multi_store"] & (panel["nb_types_prix_jour"] == 1)
    return panel


def compute_price_variation_index(df):
    monthly = (
        df.groupby(["mois", "magasin_standardise", "categorie_standardisee"], dropna=False)
        .agg(
            nb_observations=("prix_comparable", "size"),
            prix_moyen=("prix_comparable", "mean"),
            prix_median=("prix_comparable", "median"),
        )
        .reset_index()
        .sort_values(["magasin_standardise", "categorie_standardisee", "mois"], kind="stable")
    )

    monthly["variation_vs_mois_precedent_pct"] = (
        monthly.groupby(["magasin_standardise", "categorie_standardisee"])["prix_moyen"]
        .pct_change()
        .mul(100)
    )

    base_prices = (
        monthly.groupby(["magasin_standardise", "categorie_standardisee"])["prix_moyen"]
        .transform("first")
    )
    monthly["indice_base_100"] = (monthly["prix_moyen"] / base_prices) * 100
    return monthly


def compute_store_competitiveness(panel):
    comparable = panel.loc[panel["comparabilite_stricte"]].copy()
    if comparable.empty:
        return pd.DataFrame()

    daily_market = (
        comparable.groupby(["jour", "cle_matching_exacte"], dropna=False)
        .agg(
            prix_marche_min=("prix_obs", "min"),
            prix_marche_moyen=("prix_obs", "mean"),
        )
        .reset_index()
    )
    comparable = comparable.merge(daily_market, on=["jour", "cle_matching_exacte"], how="left")
    comparable["indice_vs_min"] = comparable["prix_obs"] / comparable["prix_marche_min"]
    comparable["indice_vs_moyenne"] = comparable["prix_obs"] / comparable["prix_marche_moyen"]
    comparable["ecart_vs_min_pct"] = (comparable["indice_vs_min"] - 1) * 100

    score = (
        comparable.groupby(["magasin_standardise", "mois"], dropna=False)
        .agg(
            nb_produits_comparables=("cle_matching_exacte", "nunique"),
            indice_competitivite_min=("indice_vs_min", "mean"),
            indice_competitivite_moyenne=("indice_vs_moyenne", "mean"),
            ecart_moyen_vs_min_pct=("ecart_vs_min_pct", "mean"),
        )
        .reset_index()
        .sort_values(["mois", "indice_competitivite_min", "indice_competitivite_moyenne"], kind="stable")
    )
    score["rang_competitivite"] = score.groupby("mois")["indice_competitivite_min"].rank(method="dense")
    return score


def compute_product_fluctuation(df):
    grouped = (
        df.groupby(
            ["cle_matching_exacte", "magasin_standardise", "categorie_standardisee"],
            dropna=False,
        )
        .agg(
            nom_reference=("nom_nettoye", "first"),
            nb_jours=("jour", "nunique"),
            prix_min=("prix_comparable", "min"),
            prix_max=("prix_comparable", "max"),
            prix_moyen=("prix_comparable", "mean"),
            prix_std=("prix_comparable", "std"),
            premiere_date=("jour", "min"),
            derniere_date=("jour", "max"),
        )
        .reset_index()
    )

    grouped = grouped[grouped["nb_jours"] >= 2].copy()
    if grouped.empty:
        return grouped

    grouped["amplitude_absolue"] = grouped["prix_max"] - grouped["prix_min"]
    grouped["amplitude_pct"] = (grouped["amplitude_absolue"] / grouped["prix_min"]) * 100
    grouped["coefficient_variation_pct"] = (grouped["prix_std"] / grouped["prix_moyen"]) * 100
    grouped = grouped.sort_values(
        ["coefficient_variation_pct", "amplitude_pct", "nb_jours"],
        ascending=[False, False, False],
        kind="stable",
    )
    return grouped


def compute_monthly_inflation(df):
    monthly = (
        df.groupby(["mois", "categorie_standardisee"], dropna=False)
        .agg(
            nb_observations=("prix_comparable", "size"),
            prix_moyen=("prix_comparable", "mean"),
            prix_median=("prix_comparable", "median"),
        )
        .reset_index()
        .sort_values(["categorie_standardisee", "mois"], kind="stable")
    )

    monthly["inflation_mensuelle_pct"] = (
        monthly.groupby("categorie_standardisee")["prix_moyen"]
        .pct_change()
        .mul(100)
    )
    base_prices = monthly.groupby("categorie_standardisee")["prix_moyen"].transform("first")
    monthly["indice_prix_base_100"] = (monthly["prix_moyen"] / base_prices) * 100

    global_monthly = (
        df.groupby("mois", dropna=False)
        .agg(
            nb_observations=("prix_comparable", "size"),
            prix_moyen=("prix_comparable", "mean"),
            prix_median=("prix_comparable", "median"),
        )
        .reset_index()
        .sort_values("mois", kind="stable")
    )
    global_monthly["inflation_mensuelle_pct"] = global_monthly["prix_moyen"].pct_change().mul(100)
    global_monthly["indice_prix_base_100"] = (
        global_monthly["prix_moyen"] / global_monthly["prix_moyen"].iloc[0] * 100
        if not global_monthly.empty else pd.Series(dtype=float)
    )
    return monthly, global_monthly


def build_summary(df, variation_index, competitiveness, fluctuation, inflation_global):
    latest_day = df["jour"].max()
    latest_month = df["mois"].max()

    latest_comp = competitiveness.loc[competitiveness["mois"] == latest_month].copy()
    best_store = latest_comp.sort_values("indice_competitivite_min", kind="stable").head(1)

    top_fluctuation = fluctuation.head(1)
    latest_inflation = inflation_global.sort_values("mois", kind="stable").tail(1)
    latest_variation = variation_index.sort_values("mois", kind="stable").tail(1)

    rows = [
        {
            "kpi": "date_derniere_observation",
            "valeur": str(latest_day.date()) if pd.notna(latest_day) else "",
            "details": f"{df['magasin_standardise'].nunique()} magasins, {df['cle_matching_exacte'].nunique()} cles produits",
        },
        {
            "kpi": "magasin_plus_competitif",
            "valeur": best_store["magasin_standardise"].iloc[0] if not best_store.empty else "",
            "details": (
                f"indice={best_store['indice_competitivite_min'].iloc[0]:.4f} sur {best_store['nb_produits_comparables'].iloc[0]} produits"
                if not best_store.empty else "comparaison multi-magasins insuffisante"
            ),
        },
        {
            "kpi": "produit_plus_forte_fluctuation",
            "valeur": top_fluctuation["nom_reference"].iloc[0] if not top_fluctuation.empty else "",
            "details": (
                f"{top_fluctuation['magasin_standardise'].iloc[0]} | cv={top_fluctuation['coefficient_variation_pct'].iloc[0]:.2f}% | amplitude={top_fluctuation['amplitude_pct'].iloc[0]:.2f}%"
                if not top_fluctuation.empty else "historique insuffisant pour mesurer la fluctuation"
            ),
        },
        {
            "kpi": "inflation_mensuelle_globale",
            "valeur": round(float(latest_inflation["inflation_mensuelle_pct"].iloc[0]), 2)
            if not latest_inflation.empty and pd.notna(latest_inflation["inflation_mensuelle_pct"].iloc[0]) else None,
            "details": latest_inflation["mois"].iloc[0] if not latest_inflation.empty else "",
        },
        {
            "kpi": "indice_variation_prix_dernier_mois",
            "valeur": round(float(latest_variation["indice_base_100"].mean()), 2)
            if not latest_variation.empty and latest_variation["indice_base_100"].notna().any() else None,
            "details": latest_month if pd.notna(latest_month) else "",
        },
    ]
    return pd.DataFrame(rows)


def build_quality_report(history, reliable_history, panel):
    multi_store_keys = panel.loc[panel["comparable_multi_store"], "cle_matching_exacte"].nunique()
    strict_keys = panel.loc[panel["comparabilite_stricte"], "cle_matching_exacte"].nunique()
    rows = [
        {
            "kpi": "observations_total",
            "valeur": len(history),
            "details": "lignes analytiques chargees",
        },
        {
            "kpi": "observations_retenues_kpi",
            "valeur": len(reliable_history),
            "details": "lignes retenues apres filtre de comparabilite",
        },
        {
            "kpi": "taux_retention_kpi_pct",
            "valeur": round((len(reliable_history) / len(history)) * 100, 2) if len(history) else None,
            "details": "part des observations jugees assez fiables pour les KPI",
        },
        {
            "kpi": "cles_exactes_total",
            "valeur": history["cle_matching_exacte"].nunique(),
            "details": "cles produits dans le dataset analytique",
        },
        {
            "kpi": "cles_multi_store",
            "valeur": multi_store_keys,
            "details": "cles presentes dans au moins 2 magasins le meme jour",
        },
        {
            "kpi": "cles_comparables_strictes",
            "valeur": strict_keys,
            "details": "cles multi-magasins avec type de prix coherent",
        },
        {
            "kpi": "observations_normalisees",
            "valeur": int((reliable_history["type_prix_reference"] != "raw_price").sum()) if not reliable_history.empty else 0,
            "details": "observations comparees au kg/L ou a la piece",
        },
        {
            "kpi": "observations_prix_brut",
            "valeur": int((reliable_history["type_prix_reference"] == "raw_price").sum()) if not reliable_history.empty else 0,
            "details": "observations retenues sur prix affiche exact",
        },
    ]
    return pd.DataFrame(rows)


def main():
    ensure_data_directories()
    history = load_analytics_history()
    reliable_history = filter_reliable_history(history)
    reliable_history = enrich_price_metric(reliable_history)
    panel = build_daily_reference_panel(reliable_history)

    variation_index = compute_price_variation_index(reliable_history)
    competitiveness = compute_store_competitiveness(panel)
    fluctuation = compute_product_fluctuation(reliable_history)
    inflation_categorie, inflation_globale = compute_monthly_inflation(reliable_history)
    summary = build_summary(reliable_history, variation_index, competitiveness, fluctuation, inflation_globale)
    quality_report = build_quality_report(history, reliable_history, panel)

    latest_suffix = history["jour"].max().strftime("%Y-%m-%d")
    output_variation = KPI_DATA_DIR / f"kpi_indice_variation_prix_{latest_suffix}.csv"
    output_comp = KPI_DATA_DIR / f"kpi_magasin_competitif_{latest_suffix}.csv"
    output_fluct = KPI_DATA_DIR / f"kpi_fluctuation_produits_{latest_suffix}.csv"
    output_infl_cat = KPI_DATA_DIR / f"kpi_inflation_mensuelle_categorie_{latest_suffix}.csv"
    output_infl_global = KPI_DATA_DIR / f"kpi_inflation_mensuelle_globale_{latest_suffix}.csv"
    output_summary = KPI_DATA_DIR / f"kpi_resume_{latest_suffix}.csv"
    output_panel = KPI_DATA_DIR / f"kpi_panel_journalier_{latest_suffix}.csv"
    output_quality = KPI_DATA_DIR / f"kpi_qualite_donnees_{latest_suffix}.csv"

    variation_index.to_csv(output_variation, index=False, encoding="utf-8-sig")
    competitiveness.to_csv(output_comp, index=False, encoding="utf-8-sig")
    fluctuation.to_csv(output_fluct, index=False, encoding="utf-8-sig")
    inflation_categorie.to_csv(output_infl_cat, index=False, encoding="utf-8-sig")
    inflation_globale.to_csv(output_infl_global, index=False, encoding="utf-8-sig")
    summary.to_csv(output_summary, index=False, encoding="utf-8-sig")
    panel.to_csv(output_panel, index=False, encoding="utf-8-sig")
    quality_report.to_csv(output_quality, index=False, encoding="utf-8-sig")

    print("\nKPI TERMINE")
    print(f"Historique charge : {history['jour'].nunique()} jours | {history['magasin_standardise'].nunique()} magasins")
    print(f"Historique retenu KPI : {len(reliable_history)} lignes fiables sur {len(history)}")
    print(f"Indice variation : {output_variation.name}")
    print(f"Competitivite magasin : {output_comp.name}")
    print(f"Fluctuation produits : {output_fluct.name}")
    print(f"Inflation categorie : {output_infl_cat.name}")
    print(f"Inflation globale : {output_infl_global.name}")
    print(f"Resume KPI : {output_summary.name}")
    print(f"Panel journalier : {output_panel.name}")
    print(f"Qualite donnees : {output_quality.name}")

    print("\nResume rapide :")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
