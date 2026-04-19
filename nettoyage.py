import re
import unicodedata

import pandas as pd
from project_paths import PROCESSED_DATA_DIR, RAW_DATA_DIR, ensure_data_directories, find_latest_file

COLONNES_SOURCE = [
    "nom_produit",
    "categorie",
    "prix",
    "magasin",
    "date_scraping",
    "url_produit",
]

MOTS_PARASITES = {
    "auchan",
    "sakanal",
    "diarle",
    "pet",
    "brique",
    "bouteille",
    "sachet",
    "sachets",
    "boite",
    "boites",
    "carton",
    "canette",
    "canettes",
    "flacon",
    "pot",
    "format",
    "familial",
    "promo",
    "pack",
    "barquette",
    "poche",
    "etui",
    "stick",
    "sticks",
    "tube",
}

MOTS_FAIBLES = {
    "de",
    "du",
    "des",
    "la",
    "le",
    "les",
    "au",
    "aux",
    "a",
    "et",
    "en",
    "avec",
    "sans",
    "pour",
    "sur",
    "par",
    "the",
}

EXPRESSIONS_PARASITES = [
    r"\blot\s+de\b",
    r"\bla\s+bouteille\s+de\b",
    r"\ble\s+flacon\s+de\b",
    r"\bla\s+boite\s+de\b",
    r"\ble\s+pot\s+de\b",
    r"\ble\s+sachet\s+de\b",
    r"\ble\s+pack\s+de\b",
]

CATEGORIES_MAP = {
    "epicerie": "Epicerie",
    "boissons": "Boissons",
    "hygiene & beaute": "Hygiene & Beaute",
    "hygiene & beauté": "Hygiene & Beaute",
    "produits locaux": "Produits Locaux",
    "fruits & legumes": "Fruits & Legumes",
    "fruits & légumes": "Fruits & Legumes",
}

MAGASINS_MAP = {
    "auchan": "Auchan",
    "sakanal": "Sakanal",
    "diarle": "Diarle",
}


def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


def find_latest_combined_file():
    return find_latest_file(RAW_DATA_DIR, "donnees_brutes_combinees_*.csv")


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", str(text)).strip()


def normalize_store(store):
    key = strip_accents(str(store).strip().lower())
    return MAGASINS_MAP.get(key, normalize_whitespace(store))


def normalize_category(category):
    key = strip_accents(str(category).strip().lower())
    return CATEGORIES_MAP.get(key, normalize_whitespace(category))


def normalize_quantity_text(text):
    text = str(text).lower()
    text = text.replace("\u202f", " ").replace("\xa0", " ")
    text = re.sub(r"(?<=\d),(?=\d)", ".", text)
    text = re.sub(r"\b(\d+)\s*l\s*(\d+)\b", r"\1.\2 l", text)
    text = re.sub(r"(?<=[a-z])(\d+(?:\.\d+)?)(kg|g|mg|l|cl|ml)\b", r" \1 \2", text)
    text = re.sub(r"\b(\d+(?:\.\d+)?)\s*(kg|g|mg|l|cl|ml)\s*[xX]\s*(\d+)\b", r"\3 x \1 \2", text)
    text = re.sub(r"(\d),(\d)", r"\1.\2", text)
    text = re.sub(r"\b(\d+(?:\.\d+)?)\s*litres?\b", r"\1 l", text)
    text = re.sub(r"\b(\d+(?:\.\d+)?)\s*litre\b", r"\1 l", text)
    text = re.sub(r"\b(\d+(?:\.\d+)?)\s*gr\b", r"\1 g", text)
    text = re.sub(r"\b(\d+(?:\.\d+)?)\s*grammes?\b", r"\1 g", text)
    text = re.sub(r"\b(\d+(?:\.\d+)?)\s*kilogrammes?\b", r"\1 kg", text)
    return text


def clean_product_name(raw_name):
    if not isinstance(raw_name, str):
        return ""

    name = normalize_quantity_text(raw_name)
    name = strip_accents(name.lower())

    for pattern in EXPRESSIONS_PARASITES:
        name = re.sub(pattern, " ", name)

    name = re.sub(r"[,/\\|()\-+]", " ", name)
    name = re.sub(r"[\"'`’]", " ", name)
    name = normalize_whitespace(name)

    tokens = [token for token in name.split() if token not in MOTS_PARASITES]
    return normalize_whitespace(" ".join(tokens))


def extract_quantity_info(raw_name, cleaned_name):
    text = normalize_quantity_text(f"{raw_name} {cleaned_name}")
    text = strip_accents(text)

    result = {
        "pack_count": 1,
        "quantite_par_pack": None,
        "unite_par_pack": "",
        "quantite_totale": None,
        "unite_reference": "",
        "famille_unite": "",
        "quantite_label": "",
        "prix_par_unite_reference": None,
    }

    pack_match = re.search(r"\b(\d+)\s*[xX]\s*(\d+(?:\.\d+)?)\s*(kg|g|mg|l|cl|ml)\b", text)
    if pack_match:
        pack_count = int(pack_match.group(1))
        qty = float(pack_match.group(2))
        unit = pack_match.group(3)
        result.update(convert_quantity(pack_count, qty, unit))
        return result

    single_match = re.findall(r"\b(\d+(?:\.\d+)?)\s*(kg|g|mg|l|cl|ml)\b", text)
    if single_match:
        qty, unit = single_match[-1]
        result.update(convert_quantity(1, float(qty), unit))
        return result

    count_match = re.search(r"\b(\d+)\s*(pieces|piece|pcs|pc|unites|unites?)\b", text)
    if count_match:
        count = int(count_match.group(1))
        result["pack_count"] = count
        result["quantite_par_pack"] = 1
        result["unite_par_pack"] = "piece"
        result["quantite_totale"] = count
        result["unite_reference"] = "piece"
        result["famille_unite"] = "count"
        result["quantite_label"] = f"{count}piece"
        return result

    return result


def convert_quantity(pack_count, quantity, unit):
    if unit in {"mg", "g", "kg"}:
        family = "mass"
        if unit == "mg":
            quantity_base = quantity / 1000
        elif unit == "kg":
            quantity_base = quantity * 1000
        else:
            quantity_base = quantity
        unit_ref = "g"
    else:
        family = "volume"
        if unit == "cl":
            quantity_base = quantity * 10
        elif unit == "l":
            quantity_base = quantity * 1000
        else:
            quantity_base = quantity
        unit_ref = "ml"

    total = pack_count * quantity_base
    label = f"{format_number(total)}{unit_ref}"

    return {
        "pack_count": pack_count,
        "quantite_par_pack": quantity,
        "unite_par_pack": unit,
        "quantite_totale": total,
        "unite_reference": unit_ref,
        "famille_unite": family,
        "quantite_label": label,
        "prix_par_unite_reference": None,
    }


def format_number(value):
    if value is None:
        return ""
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def build_matching_tokens(cleaned_name):
    no_qty = re.sub(r"\b\d+(?:\.\d+)?\s*(kg|g|mg|l|cl|ml)\b", " ", cleaned_name)
    no_qty = re.sub(r"\b\d+\s*(pieces|piece|pcs|pc|unites|unites?)\b", " ", no_qty)
    tokens = [
        token for token in no_qty.split()
        if token not in MOTS_FAIBLES and len(token) > 1
    ]
    return sorted(tokens)


def infer_brand(tokens):
    if not tokens:
        return ""
    if len(tokens[0]) <= 2 and len(tokens) > 1:
        return tokens[1]
    return tokens[0]


def compute_unit_prices(price, quantity_total, unit_family, unit_reference, pack_count):
    prix_par_kg_l = None
    prix_par_piece = None

    if price is None:
        return prix_par_kg_l, prix_par_piece

    if unit_family == "mass" and quantity_total:
        prix_par_kg_l = round(price / (quantity_total / 1000), 2)
    elif unit_family == "volume" and quantity_total:
        prix_par_kg_l = round(price / (quantity_total / 1000), 2)
    elif unit_family == "count" and quantity_total:
        prix_par_piece = round(price / quantity_total, 2)
    elif pack_count and pack_count > 1:
        prix_par_piece = round(price / pack_count, 2)

    return prix_par_kg_l, prix_par_piece


def confidence_level(tokens, quantity_total):
    if len(tokens) >= 3 and quantity_total:
        return "elevee"
    if len(tokens) >= 2:
        return "moyenne"
    return "faible"


def compute_matching_score(tokens, quantity_info, price):
    score = 0

    if len(tokens) >= 4:
        score += 3
    elif len(tokens) >= 3:
        score += 2
    elif len(tokens) >= 2:
        score += 1

    if quantity_info["quantite_totale"]:
        score += 2
    elif quantity_info["pack_count"] > 1:
        score += 1

    if pd.notna(price) and price > 0:
        score += 1

    return score


def infer_price_type(prix_par_kg_l, prix_par_piece):
    if pd.notna(prix_par_kg_l):
        return "unit_kg_l"
    if pd.notna(prix_par_piece):
        return "unit_piece"
    return "raw_price"


def classify_comparability(tokens, quantity_info, quality, price_type, matching_score):
    has_quantity = bool(quantity_info["quantite_totale"])
    has_pack = quantity_info["pack_count"] > 1

    if len(tokens) < 2:
        if has_quantity and len(tokens) == 1:
            return "a_revoir", "produit_monotoken_quantifie"
        return "non_comparable", "matching_trop_faible"

    if quality == "faible":
        return "non_comparable", "matching_trop_faible"

    if price_type in {"unit_kg_l", "unit_piece"} and (has_quantity or has_pack):
        if matching_score >= 4:
            return "comparable_normalise", "comparaison_unitaire_fiable"
        return "a_revoir", "comparaison_unitaire_peu_documentee"

    if price_type == "raw_price":
        if quality == "elevee" and len(tokens) >= 3:
            return "comparable_brut", "comparaison_par_prix_affiche"
        return "a_revoir", "prix_brut_sans_quantite"

    return "a_revoir", "informations_incompletes"


def enrich_row(row):
    raw_name = row.get("nom_produit", "")
    cleaned_name = clean_product_name(raw_name)
    quantity_info = extract_quantity_info(raw_name, cleaned_name)
    tokens = build_matching_tokens(cleaned_name)
    broad_key = "|".join(tokens)
    exact_key = broad_key
    if quantity_info["quantite_label"]:
        exact_key = f"{broad_key}__{quantity_info['quantite_label']}"

    brand = infer_brand(tokens)
    price = pd.to_numeric(row.get("prix"), errors="coerce")
    prix_par_kg_l, prix_par_piece = compute_unit_prices(
        price=price,
        quantity_total=quantity_info["quantite_totale"],
        unit_family=quantity_info["famille_unite"],
        unit_reference=quantity_info["unite_reference"],
        pack_count=quantity_info["pack_count"],
    )
    quality = confidence_level(tokens, quantity_info["quantite_totale"])
    matching_score = compute_matching_score(tokens, quantity_info, price)
    price_type = infer_price_type(prix_par_kg_l, prix_par_piece)
    comparability_status, comparability_reason = classify_comparability(
        tokens=tokens,
        quantity_info=quantity_info,
        quality=quality,
        price_type=price_type,
        matching_score=matching_score,
    )

    return pd.Series({
        "magasin_standardise": normalize_store(row.get("magasin", "")),
        "categorie_standardisee": normalize_category(row.get("categorie", "")),
        "nom_nettoye": cleaned_name,
        "tokens_matching": broad_key,
        "marque_probable": brand,
        "pack_count": quantity_info["pack_count"],
        "quantite_par_pack": quantity_info["quantite_par_pack"],
        "unite_par_pack": quantity_info["unite_par_pack"],
        "quantite_totale_reference": quantity_info["quantite_totale"],
        "unite_reference": quantity_info["unite_reference"],
        "famille_unite": quantity_info["famille_unite"],
        "quantite_standardisee": quantity_info["quantite_label"],
        "cle_matching_large": broad_key,
        "cle_matching_exacte": exact_key,
        "prix_par_kg_ou_l": prix_par_kg_l,
        "prix_par_piece": prix_par_piece,
        "qualite_matching": quality,
        "score_matching": matching_score,
        "type_prix_reference": price_type,
        "statut_comparabilite": comparability_status,
        "motif_comparabilite": comparability_reason,
    })


def build_product_reference(df):
    grouped = (
        df.groupby(
            ["cle_matching_exacte", "cle_matching_large", "categorie_standardisee"],
            dropna=False,
        )
        .agg(
            nb_lignes=("nom_produit", "size"),
            nb_magasins=("magasin_standardise", "nunique"),
            magasins=("magasin_standardise", lambda s: " | ".join(sorted(set(s)))),
            nom_reference=("nom_nettoye", "first"),
            marque_probable=("marque_probable", "first"),
            quantite_standardisee=("quantite_standardisee", "first"),
            famille_unite=("famille_unite", "first"),
            prix_min=("prix", "min"),
            prix_max=("prix", "max"),
            score_matching_max=("score_matching", "max"),
            statut_comparabilite=("statut_comparabilite", lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0]),
        )
        .reset_index()
    )
    return grouped.sort_values(
        by=["nb_magasins", "categorie_standardisee", "nom_reference"],
        ascending=[False, True, True],
        kind="stable",
    )


def build_exact_price_panel(df):
    comparable_df = df.loc[
        df["statut_comparabilite"].isin(["comparable_normalise", "comparable_brut"])
    ].copy()
    if comparable_df.empty:
        return pd.DataFrame()

    grouped = (
        comparable_df.groupby(
            ["cle_matching_exacte", "categorie_standardisee", "quantite_standardisee", "magasin_standardise"],
            dropna=False,
        )
        .agg(
            nom_reference=("nom_nettoye", "first"),
            prix_min=("prix", "min"),
            prix_moyen=("prix", "mean"),
            prix_par_kg_ou_l=("prix_par_kg_ou_l", "mean"),
            prix_par_piece=("prix_par_piece", "mean"),
            type_prix_reference=("type_prix_reference", "first"),
        )
        .reset_index()
    )

    coverage = (
        grouped.groupby("cle_matching_exacte")["magasin_standardise"]
        .nunique()
        .rename("nb_magasins")
        .reset_index()
    )
    grouped = grouped.merge(coverage, on="cle_matching_exacte", how="left")
    type_coverage = (
        grouped.groupby("cle_matching_exacte")["type_prix_reference"]
        .nunique()
        .rename("nb_types_prix")
        .reset_index()
    )
    grouped = grouped.merge(type_coverage, on="cle_matching_exacte", how="left")
    grouped = grouped[grouped["nb_magasins"] >= 2].copy()
    grouped = grouped[grouped["nb_types_prix"] == 1].copy()
    if grouped.empty:
        return pd.DataFrame()

    price_panel = grouped.pivot_table(
        index=[
            "cle_matching_exacte",
            "categorie_standardisee",
            "quantite_standardisee",
            "nom_reference",
            "nb_magasins",
            "type_prix_reference",
        ],
        columns="magasin_standardise",
        values="prix_min",
        aggfunc="min",
    ).reset_index()
    return price_panel


def main():
    ensure_data_directories()
    source_file = find_latest_combined_file()
    if source_file is None:
        print("Aucun fichier donnees_brutes_combinees_*.csv trouve.")
        return

    print(f"Chargement du fichier source : {source_file.name}")
    df = pd.read_csv(source_file)

    missing = [col for col in COLONNES_SOURCE if col not in df.columns]
    if missing:
        print(f"Colonnes manquantes dans le fichier source : {missing}")
        return

    df = df[COLONNES_SOURCE].copy()
    df["prix"] = pd.to_numeric(df["prix"], errors="coerce")
    df["date_scraping"] = pd.to_datetime(df["date_scraping"], errors="coerce")
    df = df.dropna(subset=["nom_produit", "prix", "magasin", "date_scraping"]).copy()

    enriched = df.apply(enrich_row, axis=1)
    df = pd.concat([df, enriched], axis=1)

    df["annee"] = df["date_scraping"].dt.year
    df["mois"] = df["date_scraping"].dt.month
    df["mois_nom"] = df["date_scraping"].dt.strftime("%Y-%m")
    df["trimestre"] = df["date_scraping"].dt.to_period("Q").astype(str)
    df["jour"] = df["date_scraping"].dt.date.astype(str)

    df = df.drop_duplicates(
        subset=[
            "magasin_standardise",
            "jour",
            "cle_matching_exacte",
            "prix",
            "url_produit",
        ],
        keep="first",
    )

    product_reference = build_product_reference(df)
    exact_price_panel = build_exact_price_panel(df)

    suffix = source_file.stem.replace("donnees_brutes_combinees_", "")
    output_main = PROCESSED_DATA_DIR / f"donnees_analytiques_kpi_{suffix}.csv"
    output_reference = PROCESSED_DATA_DIR / f"referentiel_produits_{suffix}.csv"
    output_panel = PROCESSED_DATA_DIR / f"panel_prix_exact_{suffix}.csv"

    ordered_columns = [
        "nom_produit",
        "nom_nettoye",
        "marque_probable",
        "categorie_standardisee",
        "magasin_standardise",
        "prix",
        "prix_par_kg_ou_l",
        "prix_par_piece",
        "pack_count",
        "quantite_par_pack",
        "unite_par_pack",
        "quantite_totale_reference",
        "unite_reference",
        "famille_unite",
        "quantite_standardisee",
        "cle_matching_large",
        "cle_matching_exacte",
        "qualite_matching",
        "score_matching",
        "type_prix_reference",
        "statut_comparabilite",
        "motif_comparabilite",
        "date_scraping",
        "annee",
        "mois",
        "mois_nom",
        "trimestre",
        "jour",
        "url_produit",
    ]

    df = df[ordered_columns].sort_values(
        by=["date_scraping", "magasin_standardise", "categorie_standardisee", "nom_nettoye"],
        kind="stable",
    )

    df.to_csv(output_main, index=False, encoding="utf-8-sig")
    product_reference.to_csv(output_reference, index=False, encoding="utf-8-sig")

    if not exact_price_panel.empty:
        exact_price_panel.to_csv(output_panel, index=False, encoding="utf-8-sig")

    print("\nNETTOYAGE TERMINE")
    print(f"Fichier analytique KPI : {output_main.name}")
    print(f"Referentiel produits : {output_reference.name}")
    if not exact_price_panel.empty:
        print(f"Panel prix exact : {output_panel.name}")
    else:
        print("Panel prix exact non genere : aucune cle exacte partagee entre au moins 2 magasins.")

    print("\nRepartition par magasin :")
    print(df["magasin_standardise"].value_counts())

    print("\nRepartition qualite matching :")
    print(df["qualite_matching"].value_counts())

    print("\nRepartition statut comparabilite :")
    print(df["statut_comparabilite"].value_counts())

    print("\nApercu :")
    print(
        df[
            [
                "nom_produit",
                "nom_nettoye",
                "quantite_standardisee",
                "cle_matching_exacte",
                "statut_comparabilite",
                "prix",
                "prix_par_kg_ou_l",
                "magasin_standardise",
            ]
        ].head(12).to_string(index=False)
    )


if __name__ == "__main__":
    main()
