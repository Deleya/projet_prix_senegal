import pandas as pd
import streamlit as st
from project_paths import KPI_DATA_DIR, PROCESSED_DATA_DIR, find_latest_file


st.set_page_config(
    page_title="Dashboard Prix Senegal",
    page_icon=":shopping_trolley:",
    layout="wide",
)

@st.cache_data(show_spinner=False)
def load_csv(path_str: str) -> pd.DataFrame:
    return pd.read_csv(path_str)


def load_latest_datasets():
    targets = {
        "analytics": find_latest_file(PROCESSED_DATA_DIR, "donnees_analytiques_kpi_*.csv"),
        "panel": find_latest_file(KPI_DATA_DIR, "kpi_panel_journalier_*.csv"),
        "competitiveness": find_latest_file(KPI_DATA_DIR, "kpi_magasin_competitif_*.csv"),
        "summary": find_latest_file(KPI_DATA_DIR, "kpi_resume_*.csv"),
        "variation": find_latest_file(KPI_DATA_DIR, "kpi_indice_variation_prix_*.csv"),
        "inflation_category": find_latest_file(KPI_DATA_DIR, "kpi_inflation_mensuelle_categorie_*.csv"),
        "inflation_global": find_latest_file(KPI_DATA_DIR, "kpi_inflation_mensuelle_globale_*.csv"),
        "fluctuation": find_latest_file(KPI_DATA_DIR, "kpi_fluctuation_produits_*.csv"),
        "quality": find_latest_file(KPI_DATA_DIR, "kpi_qualite_donnees_*.csv"),
    }

    missing = [
        name for name, path in targets.items()
        if path is None and name != "quality"
    ]
    if missing:
        raise FileNotFoundError(
            "Fichiers manquants: " + ", ".join(missing)
        )

    datasets = {
        name: load_csv(str(path))
        for name, path in targets.items()
        if path is not None
    }
    datasets["paths"] = targets
    return datasets


def prepare_datasets(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    analytics = datasets["analytics"].copy()
    panel = datasets["panel"].copy()
    competitiveness = datasets["competitiveness"].copy()
    variation = datasets["variation"].copy()
    inflation_category = datasets["inflation_category"].copy()
    inflation_global = datasets["inflation_global"].copy()
    fluctuation = datasets["fluctuation"].copy()
    quality = datasets.get("quality", pd.DataFrame()).copy()

    for frame in (analytics, panel):
        for date_col in ("jour", "date_scraping"):
            if date_col in frame.columns:
                frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")

    numeric_columns = [
        (analytics, ["prix", "prix_par_kg_ou_l", "prix_par_piece", "score_matching"]),
        (panel, ["prix_obs", "nb_magasins_jour", "nb_types_prix_jour", "score_matching_moyen"]),
        (competitiveness, [
            "nb_produits_comparables",
            "indice_competitivite_min",
            "indice_competitivite_moyenne",
            "ecart_moyen_vs_min_pct",
            "rang_competitivite",
        ]),
        (variation, [
            "nb_observations",
            "prix_moyen",
            "prix_median",
            "variation_vs_mois_precedent_pct",
            "indice_base_100",
        ]),
        (inflation_category, [
            "nb_observations",
            "prix_moyen",
            "prix_median",
            "inflation_mensuelle_pct",
            "indice_prix_base_100",
        ]),
        (inflation_global, [
            "nb_observations",
            "prix_moyen",
            "prix_median",
            "inflation_mensuelle_pct",
            "indice_prix_base_100",
        ]),
        (fluctuation, [
            "nb_jours",
            "prix_min",
            "prix_max",
            "prix_moyen",
            "prix_std",
            "amplitude_absolue",
            "amplitude_pct",
            "coefficient_variation_pct",
        ]),
        (quality, ["valeur"]),
    ]

    for frame, cols in numeric_columns:
        for col in cols:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")

    if "comparable_multi_store" in panel.columns:
        panel["comparable_multi_store"] = (
            panel["comparable_multi_store"].astype(str).str.lower() == "true"
        )
    else:
        panel["comparable_multi_store"] = False
    if "comparabilite_stricte" in panel.columns:
        panel["comparabilite_stricte"] = (
            panel["comparabilite_stricte"].astype(str).str.lower() == "true"
        )
    else:
        panel["comparabilite_stricte"] = panel["comparable_multi_store"]

    analytics["prix_affiche"] = analytics["prix"]
    analytics["prix_normalise"] = analytics["prix_par_kg_ou_l"].where(
        analytics["prix_par_kg_ou_l"].notna(),
        analytics["prix_par_piece"],
    )

    datasets["analytics"] = analytics
    datasets["panel"] = panel
    datasets["competitiveness"] = competitiveness
    datasets["variation"] = variation
    datasets["inflation_category"] = inflation_category
    datasets["inflation_global"] = inflation_global
    datasets["fluctuation"] = fluctuation
    datasets["quality"] = quality
    return datasets


def format_fcfa(value) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:,.0f} FCFA".replace(",", " ")


def format_pct(value) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:.1f}%"


def build_price_comparison(
    analytics: pd.DataFrame,
    stores: list[str],
    price_mode: str,
) -> pd.DataFrame:
    comparable = analytics.loc[
        analytics["magasin_standardise"].isin(stores)
        & analytics["cle_matching_exacte"].notna()
    ].copy()
    if comparable.empty:
        return pd.DataFrame()

    if "statut_comparabilite" in comparable.columns:
        comparable = comparable.loc[
            comparable["statut_comparabilite"].eq("comparable_normalise")
        ].copy()
    if comparable.empty:
        return pd.DataFrame()

    if price_mode == "prix_normalise":
        value_column = "prix_normalise"
        if "type_prix_reference" in comparable.columns:
            comparable = comparable.loc[comparable["type_prix_reference"] != "raw_price"].copy()
    else:
        value_column = "prix_affiche"

    comparable = comparable.loc[comparable[value_column].notna()].copy()
    comparable["store_count_selected"] = comparable.groupby(
        ["jour", "cle_matching_exacte"]
    )["magasin_standardise"].transform("nunique")
    if "type_prix_reference" in comparable.columns:
        comparable["price_type_count_selected"] = comparable.groupby(
            ["jour", "cle_matching_exacte"]
        )["type_prix_reference"].transform("nunique")
        comparable = comparable.loc[comparable["price_type_count_selected"] == 1].copy()
    comparable = comparable.loc[comparable["store_count_selected"] >= 2].copy()
    if comparable.empty:
        return pd.DataFrame()

    pivot = (
        comparable.pivot_table(
            index=["jour", "categorie_standardisee", "cle_matching_exacte", "nom_produit"],
            columns="magasin_standardise",
            values=value_column,
            aggfunc="mean",
        )
        .reset_index()
    )

    price_columns = [store for store in stores if store in pivot.columns]
    if not price_columns:
        return pd.DataFrame()

    pivot["nb_magasins_selectionnes_disponibles"] = pivot[price_columns].notna().sum(axis=1)
    pivot["prix_min"] = pivot[price_columns].min(axis=1, skipna=True)
    pivot["prix_max"] = pivot[price_columns].max(axis=1, skipna=True)
    pivot["ecart_fcfa"] = pivot["prix_max"] - pivot["prix_min"]
    pivot["ecart_pct"] = (pivot["ecart_fcfa"] / pivot["prix_min"]) * 100
    pivot["mode_prix"] = price_mode

    winner_labels = []
    winner_count = []
    for _, row in pivot.iterrows():
        winners = [
            store
            for store in price_columns
            if pd.notna(row.get(store)) and row.get(store) == row["prix_min"]
        ]
        winner_count.append(len(winners))
        winner_labels.append(", ".join(winners))
    pivot["magasin_moins_cher"] = winner_labels
    pivot["nb_gagnants"] = winner_count
    return pivot.sort_values(["ecart_fcfa", "nom_produit"], ascending=[False, True], kind="stable")


def build_head_to_head_summary(comparison: pd.DataFrame, stores: list[str]) -> pd.DataFrame:
    if comparison.empty:
        return pd.DataFrame(columns=["magasin", "victoires", "taux_victoire_pct", "prix_moyen"])

    rows = []
    valid = comparison.loc[comparison["nb_gagnants"] >= 1].copy()
    for store in stores:
        if store not in comparison.columns:
            continue
        wins = valid["magasin_moins_cher"].str.contains(store, regex=False, na=False).sum()
        available = comparison[store].notna().sum()
        rows.append(
            {
                "magasin": store,
                "victoires": wins,
                "taux_victoire_pct": (wins / len(valid) * 100) if len(valid) else None,
                "prix_moyen": comparison[store].mean(),
                "couverture_produits": available,
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(
        ["taux_victoire_pct", "prix_moyen"],
        ascending=[False, True],
        kind="stable",
    )


def build_category_scorecard(comparison: pd.DataFrame, stores: list[str]) -> pd.DataFrame:
    if comparison.empty:
        return pd.DataFrame()

    rows = []
    for category, group in comparison.groupby("categorie_standardisee", dropna=False):
        for store in stores:
            if store not in group.columns:
                continue
            wins = group["magasin_moins_cher"].str.contains(store, regex=False, na=False).sum()
            rows.append(
                {
                    "categorie": category,
                    "magasin": store,
                    "produits_comparables": group[store].notna().sum(),
                    "victoires_prix": wins,
                    "taux_victoire_pct": (wins / len(group) * 100) if len(group) else None,
                    "prix_moyen_obs": group[store].mean(),
                }
            )

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(
        ["categorie", "taux_victoire_pct", "prix_moyen_obs"],
        ascending=[True, False, True],
        kind="stable",
    )


def build_store_gap_table(comparison: pd.DataFrame, store: str) -> pd.DataFrame:
    if comparison.empty or store not in comparison.columns:
        return pd.DataFrame()

    gap = comparison.loc[comparison[store].notna()].copy()
    if gap.empty:
        return pd.DataFrame()

    gap["surcout_vs_min_fcfa"] = gap[store] - gap["prix_min"]
    gap["surcout_vs_min_pct"] = (gap["surcout_vs_min_fcfa"] / gap["prix_min"]) * 100
    gap = gap.sort_values(
        ["surcout_vs_min_fcfa", "surcout_vs_min_pct"],
        ascending=[False, False],
        kind="stable",
    )
    return gap


def show_overview_metrics(analytics: pd.DataFrame, panel: pd.DataFrame, competition_subset: pd.DataFrame):
    comparable_keys = panel.loc[panel["comparabilite_stricte"], "cle_matching_exacte"].nunique()
    latest_day = analytics["jour"].max()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Derniere observation", latest_day.strftime("%Y-%m-%d") if pd.notna(latest_day) else "-")
    col2.metric("Magasins couverts", analytics["magasin_standardise"].nunique())
    col3.metric("Produits matchés", analytics["cle_matching_exacte"].nunique())
    col4.metric("Produits comparables", comparable_keys)

    if not competition_subset.empty:
        leader = competition_subset.sort_values("indice_competitivite_min", kind="stable").iloc[0]
        cols = st.columns(3)
        cols[0].metric(
            "Magasin le plus competitif",
            leader["magasin_standardise"],
            delta=f"{leader['ecart_moyen_vs_min_pct']:.1f}% vs prix mini marche",
            delta_color="inverse",
        )
        cols[1].metric(
            "Indice competitivite moyen",
            f"{leader['indice_competitivite_min']:.3f}",
        )
        cols[2].metric(
            "Produits vraiment comparés",
            int(leader["nb_produits_comparables"]) if pd.notna(leader["nb_produits_comparables"]) else 0,
        )


def show_quality_metrics(analytics: pd.DataFrame, panel: pd.DataFrame, quality: pd.DataFrame):
    st.subheader("Fiabilite des donnees")

    reliable = analytics.loc[
        analytics["statut_comparabilite"].eq("comparable_normalise")
    ].copy() if "statut_comparabilite" in analytics.columns else analytics.copy()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Observations KPI retenues", len(reliable))
    col2.metric(
        "Taux retention",
        format_pct((len(reliable) / len(analytics) * 100) if len(analytics) else None),
    )
    col3.metric("Cles multi-store", panel.loc[panel["comparable_multi_store"], "cle_matching_exacte"].nunique())
    col4.metric("Cles strictes", panel.loc[panel["comparabilite_stricte"], "cle_matching_exacte"].nunique())

    if not quality.empty and {"kpi", "valeur", "details"}.issubset(quality.columns):
        with st.expander("Voir le rapport global exporte", expanded=False):
            st.dataframe(quality, width="stretch", hide_index=True)

    if "statut_comparabilite" in analytics.columns and "type_prix_reference" in analytics.columns:
        col_left, col_right = st.columns(2)
        with col_left:
            breakdown = (
                analytics["statut_comparabilite"]
                .value_counts(dropna=False)
                .rename_axis("statut")
                .reset_index(name="nb_observations")
            )
            st.dataframe(breakdown, width="stretch", hide_index=True)
        with col_right:
            review_zone = analytics.loc[
                analytics["statut_comparabilite"].eq("a_revoir")
            ].copy()
            base_prix = (
                review_zone["motif_comparabilite"]
                .value_counts(dropna=False)
                .rename_axis("motif_comparabilite")
                .reset_index(name="nb_observations")
            )
            st.dataframe(base_prix, width="stretch", hide_index=True)


def main():
    st.title("Dashboard Streamlit - Prix de consommation au Senegal")
    st.caption(
        "Analyse comparee des prix entre Auchan, Sakanal et Diarle a partir des fichiers matchés et KPI les plus recents."
    )

    try:
        datasets = prepare_datasets(load_latest_datasets())
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    analytics = datasets["analytics"]
    panel = datasets["panel"]
    competitiveness = datasets["competitiveness"]
    variation = datasets["variation"]
    inflation_category = datasets["inflation_category"]
    inflation_global = datasets["inflation_global"]
    fluctuation = datasets["fluctuation"]
    quality = datasets.get("quality", pd.DataFrame())
    paths = datasets["paths"]

    available_stores = sorted(analytics["magasin_standardise"].dropna().unique().tolist())
    available_categories = sorted(analytics["categorie_standardisee"].dropna().unique().tolist())
    default_stores = [store for store in ["Auchan", "Sakanal", "Diarle"] if store in available_stores] or available_stores

    st.sidebar.header("Filtres")
    selected_stores = st.sidebar.multiselect(
        "Magasins a comparer",
        options=available_stores,
        default=default_stores,
    )
    selected_categories = st.sidebar.multiselect(
        "Categories",
        options=available_categories,
        default=[],
    )
    search_term = st.sidebar.text_input("Recherche produit", placeholder="lait, huile, riz...")
    only_full_coverage = st.sidebar.checkbox(
        "Montrer seulement les produits presents dans tous les magasins selectionnes",
        value=False,
    )
    price_mode_label = st.sidebar.radio(
        "Mode de comparaison",
        options=["Prix produit", "Prix normalise (kg/L ou piece)"],
        index=0,
    )

    st.sidebar.divider()
    st.sidebar.caption("Sources chargees automatiquement")
    for label, path in paths.items():
        if path is None:
            continue
        st.sidebar.write(f"{label}: `{path.name}`")

    if not selected_stores:
        st.warning("Selectionne au moins un magasin pour afficher la comparaison.")
        st.stop()

    price_mode = "prix_affiche" if price_mode_label == "Prix produit" else "prix_normalise"

    filtered_analytics = analytics.loc[analytics["magasin_standardise"].isin(selected_stores)].copy()
    filtered_panel = panel.loc[panel["magasin_standardise"].isin(selected_stores)].copy()
    filtered_competitiveness = competitiveness.loc[
        competitiveness["magasin_standardise"].isin(selected_stores)
    ].copy()

    if selected_categories:
        filtered_analytics = filtered_analytics.loc[
            filtered_analytics["categorie_standardisee"].isin(selected_categories)
        ].copy()
        filtered_panel = filtered_panel.loc[
            filtered_panel["categorie_standardisee"].isin(selected_categories)
        ].copy()

    if search_term:
        mask = filtered_panel["nom_reference"].fillna("").str.contains(search_term, case=False, na=False)
        filtered_panel = filtered_panel.loc[mask].copy()
        filtered_analytics = filtered_analytics.loc[
            filtered_analytics["nom_nettoye"].fillna("").str.contains(search_term, case=False, na=False)
        ].copy()

    latest_month = filtered_competitiveness["mois"].max() if not filtered_competitiveness.empty else None
    latest_competition = filtered_competitiveness.loc[
        filtered_competitiveness["mois"] == latest_month
    ].copy()

    show_overview_metrics(filtered_analytics, filtered_panel, latest_competition)
    st.divider()
    show_quality_metrics(filtered_analytics, filtered_panel, quality)

    comparison = build_price_comparison(filtered_analytics, selected_stores, price_mode)
    if only_full_coverage and not comparison.empty:
        comparison = comparison.loc[
            comparison["nb_magasins_selectionnes_disponibles"] == len(selected_stores)
        ].copy()

    head_to_head = build_head_to_head_summary(comparison, selected_stores)
    category_scorecard = build_category_scorecard(comparison, selected_stores)

    st.divider()
    st.subheader("Qui est vraiment le moins cher ?")
    st.caption(
        "Affichage actuel: "
        + ("prix reel du produit" if price_mode == "prix_affiche" else "prix normalise pour comparer au kg/L ou a la piece")
    )

    if comparison.empty:
        st.info(
            "Pas assez de produits comparables avec les filtres actuels. Elargis les magasins, categories ou la recherche."
        )
    else:
        if not head_to_head.empty:
            leader = head_to_head.iloc[0]
            cols = st.columns(4)
            cols[0].metric("Leader prix", leader["magasin"])
            cols[1].metric("Taux de victoire", format_pct(leader["taux_victoire_pct"]))
            cols[2].metric("Produits compares", len(comparison))
            cols[3].metric("Ecart moyen max-min", format_fcfa(comparison["ecart_fcfa"].mean()))

            st.markdown(
                f"Sur **{len(comparison)} produits comparables**, **{leader['magasin']}** ressort comme l'enseigne la plus souvent la moins chere."
            )

            summary_view = head_to_head.copy()
            summary_view["taux_victoire_pct"] = summary_view["taux_victoire_pct"].map(format_pct)
            summary_view["prix_moyen"] = summary_view["prix_moyen"].map(format_fcfa)
            st.dataframe(summary_view, width='stretch', hide_index=True)

        price_gap_chart = (
            comparison.groupby("magasin_moins_cher", dropna=False)["cle_matching_exacte"]
            .count()
            .rename("nb_produits")
            .sort_values(ascending=False)
        )
        st.bar_chart(price_gap_chart)

    st.divider()
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Classement competitivite globale")
        if latest_competition.empty:
            st.info("Aucune mesure de competitivite disponible pour les filtres actuels.")
        else:
            ranking = latest_competition.sort_values(
                "indice_competitivite_min",
                ascending=True,
                kind="stable",
            ).copy()
            ranking_view = ranking[
                [
                    "magasin_standardise",
                    "nb_produits_comparables",
                    "indice_competitivite_min",
                    "ecart_moyen_vs_min_pct",
                    "rang_competitivite",
                ]
            ].copy()
            ranking_view["indice_competitivite_min"] = ranking_view["indice_competitivite_min"].round(3)
            ranking_view["ecart_moyen_vs_min_pct"] = ranking_view["ecart_moyen_vs_min_pct"].map(format_pct)
            st.dataframe(ranking_view, width='stretch', hide_index=True)

    with col_right:
        st.subheader("Ou chaque magasin gagne par categorie")
        if category_scorecard.empty:
            st.info("Le score par categorie apparaitra des qu'il y aura assez de produits comparables.")
        else:
            best_by_category = (
                category_scorecard.sort_values(
                    ["categorie", "taux_victoire_pct", "prix_moyen_obs"],
                    ascending=[True, False, True],
                    kind="stable",
                )
                .drop_duplicates(subset=["categorie"], keep="first")
                .copy()
            )
            best_by_category["taux_victoire_pct"] = best_by_category["taux_victoire_pct"].map(format_pct)
            best_by_category["prix_moyen_obs"] = best_by_category["prix_moyen_obs"].map(format_fcfa)
            st.dataframe(best_by_category, width='stretch', hide_index=True)

    st.divider()
    st.subheader("Produits a plus fort ecart de prix")
    if comparison.empty:
        st.info("Aucun ecart produit a afficher.")
    else:
        product_table = comparison.copy()
        for store in selected_stores:
            if store in product_table.columns:
                product_table[store] = product_table[store].map(format_fcfa)
        product_table["prix_min"] = product_table["prix_min"].map(format_fcfa)
        product_table["prix_max"] = product_table["prix_max"].map(format_fcfa)
        product_table["ecart_fcfa"] = product_table["ecart_fcfa"].map(format_fcfa)
        product_table["ecart_pct"] = product_table["ecart_pct"].map(format_pct)

        columns_to_show = [
            "nom_produit",
            "categorie_standardisee",
            "magasin_moins_cher",
            "prix_min",
            "prix_max",
            "ecart_fcfa",
            "ecart_pct",
        ] + [store for store in selected_stores if store in product_table.columns]

        st.dataframe(
            product_table[columns_to_show].head(100),
            width='stretch',
            hide_index=True,
        )

    st.divider()
    focus_store = st.selectbox(
        "Zoom sur une enseigne pour voir ou elle laisse le plus d'argent sur la table",
        options=selected_stores,
    )
    focus_gap = build_store_gap_table(comparison, focus_store)
    if focus_gap.empty:
        st.info("Aucun ecart detaille disponible pour cette enseigne.")
    else:
        cols = st.columns(3)
        cols[0].metric("Surcout moyen vs meilleur prix", format_fcfa(focus_gap["surcout_vs_min_fcfa"].mean()))
        cols[1].metric("Surcout median", format_fcfa(focus_gap["surcout_vs_min_fcfa"].median()))
        cols[2].metric(
            "Produits ou l'enseigne n'est pas la moins chere",
            int((focus_gap["surcout_vs_min_fcfa"] > 0).sum()),
        )

        display_gap = focus_gap[
            [
                "nom_produit",
                "categorie_standardisee",
                "magasin_moins_cher",
                focus_store,
                "prix_min",
                "surcout_vs_min_fcfa",
                "surcout_vs_min_pct",
            ]
        ].copy()
        display_gap[focus_store] = display_gap[focus_store].map(format_fcfa)
        display_gap["prix_min"] = display_gap["prix_min"].map(format_fcfa)
        display_gap["surcout_vs_min_fcfa"] = display_gap["surcout_vs_min_fcfa"].map(format_fcfa)
        display_gap["surcout_vs_min_pct"] = display_gap["surcout_vs_min_pct"].map(format_pct)
        st.dataframe(display_gap.head(50), width='stretch', hide_index=True)

    st.divider()
    bottom_left, bottom_right = st.columns(2)

    with bottom_left:
        st.subheader("Prix moyen observe par categorie")
        avg_price = (
            filtered_analytics.groupby(["categorie_standardisee", "magasin_standardise"], dropna=False)["prix"]
            .mean()
            .reset_index()
            .sort_values(["categorie_standardisee", "prix"], ascending=[True, True], kind="stable")
        )
        if avg_price.empty:
            st.info("Pas de prix moyens disponibles avec ces filtres.")
        else:
            avg_price["prix"] = avg_price["prix"].round(0)
            st.dataframe(avg_price, width='stretch', hide_index=True)

    with bottom_right:
        st.subheader("Tendance prix / inflation")
        inflation_view = inflation_category.copy()
        if selected_categories:
            inflation_view = inflation_view.loc[
                inflation_view["categorie_standardisee"].isin(selected_categories)
            ].copy()

        if inflation_view.empty:
            st.info("Pas assez d'historique pour une tendance d'inflation par categorie.")
        else:
            chart_source = inflation_view.pivot_table(
                index="mois",
                columns="categorie_standardisee",
                values="indice_prix_base_100",
                aggfunc="mean",
            )
            st.line_chart(chart_source)

        if not inflation_global.empty:
            latest_global = inflation_global.sort_values("mois", kind="stable").tail(1).iloc[0]
            st.caption(
                "Inflation mensuelle globale la plus recente: "
                f"{format_pct(latest_global.get('inflation_mensuelle_pct'))} "
                f"({latest_global.get('mois', '-')})"
            )

    st.divider()
    st.subheader("Points de vigilance")
    warnings = []
    if comparison.empty:
        warnings.append("Les filtres actifs ne laissent pas assez de produits communs pour une vraie comparaison magasin contre magasin.")
    if fluctuation.empty:
        warnings.append("La fluctuation produit n'est pas encore interpretable: il faut plusieurs jours d'historique pour mesurer une vraie volatilite.")
    if variation["variation_vs_mois_precedent_pct"].notna().sum() == 0:
        warnings.append("L'indice de variation mensuelle est encore au point de depart: un seul mois est disponible dans l'historique actuel.")
    if not warnings:
        warnings.append("Les donnees actuelles sont suffisamment coherentes pour une lecture concurrentielle de premier niveau.")

    for item in warnings:
        st.write(f"- {item}")


if __name__ == "__main__":
    main()
