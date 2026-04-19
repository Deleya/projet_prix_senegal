import random
import re
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from project_paths import RAW_DATA_DIR, ensure_data_directories

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    )
}

CATEGORIES = [
    {"url": "https://sakanal.sn/fr/111-epicerie", "categorie": "Epicerie"},
    {"url": "https://sakanal.sn/fr/56-boissons", "categorie": "Boissons"},
    {"url": "https://sakanal.sn/fr/48-hygiene-et-beaute", "categorie": "Hygiene & Beaute"},
    {"url": "https://sakanal.sn/fr/83-produits-locaux", "categorie": "Produits Locaux"},
    {"url": "https://sakanal.sn/fr/67-produits-frais", "categorie": "Produits Frais"},
    {"url": "https://sakanal.sn/fr/63-bebe-puericulture", "categorie": "Bebe & Puériculture"},
]

MAX_PAGES_PER_CATEGORY = 30


def clean_price(price_text):
    if not price_text:
        return None

    match = re.search(r"(\d[\d\s.,]*?)\s*(?:CFA|FCFA)", str(price_text), re.IGNORECASE)
    if not match:
        return None

    number_str = re.sub(r"[\s.,]", "", match.group(1))
    try:
        return float(number_str)
    except ValueError:
        return None


def clean_name(name):
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.strip())


def extract_price_from_h2(h2_tag):
    if not h2_tag:
        return None

    current = h2_tag.next_sibling
    for _ in range(25):
        if current is None:
            break

        text = current.get_text(strip=True) if hasattr(current, "get_text") else str(current)
        if "FCFA" in text.upper():
            return text

        current = current.next_sibling

    return None


def extract_current_page_number(soup):
    active_selectors = [
        ".pagination .current a",
        ".pagination .current",
        ".pagination .active a",
        ".pagination .active",
        "li.current a",
        "li.current",
        "li.active a",
        "li.active",
    ]

    for selector in active_selectors:
        node = soup.select_one(selector)
        if not node:
            continue

        match = re.search(r"\b(\d+)\b", node.get_text(" ", strip=True))
        if match:
            return int(match.group(1))

    return 1


def has_next_page(soup):
    next_selectors = [
        ".pagination .next:not(.disabled) a",
        ".pagination_next:not(.disabled) a",
        "li.next:not(.disabled) a",
        "a[rel='next']",
    ]

    for selector in next_selectors:
        if soup.select_one(selector):
            return True

    for link in soup.select(".pagination a[href], li a[href]"):
        href = link.get("href", "")
        text = link.get_text(" ", strip=True).lower()
        classes = " ".join(link.get("class", [])).lower()

        if "page=" in href and ("next" in text or "suivant" in text or "next" in classes):
            return True

    return False


def page_overlap_ratio(page_signature, seen_signatures):
    if not page_signature:
        return 0.0

    repeated = sum(1 for item in page_signature if item in seen_signatures)
    return repeated / len(page_signature)


def build_page_signature(product_links):
    signature = []
    for a_tag in product_links:
        name = clean_name(a_tag.get_text())
        href = a_tag.get("href", "").strip()
        if name or href:
            signature.append((name, href))
    return tuple(signature)


def extract_product_links(soup):
    selectors = [
        "article.product-miniature h2 a",
        ".product-miniature h2 a",
        ".js-product-miniature h2 a",
        "h2 a",
    ]

    for selector in selectors:
        product_links = soup.select(selector)
        if product_links:
            return product_links

    return []


def scrape_sakanal():
    ensure_data_directories()
    all_data = []
    date_today = datetime.now().strftime("%Y-%m-%d")
    session = requests.Session()

    for cat in CATEGORIES:
        print(f"\nDebut du scraping -> Categorie : {cat['categorie']}")
        base_url = cat["url"]
        previous_signature = None
        seen_signatures_in_category = set()

        for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
            url = f"{base_url}?page={page}" if page > 1 else base_url
            print(f"   Page {page} -> {url}")

            try:
                response = session.get(url, headers=HEADERS, timeout=15)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                product_links = extract_product_links(soup)
                total_elements = len(product_links)
                page_signature = build_page_signature(product_links)
                served_page = extract_current_page_number(soup)
                next_page_exists = has_next_page(soup)
                overlap_ratio = page_overlap_ratio(page_signature, seen_signatures_in_category)

                products_found = 0
                seen_on_page = set()
                new_urls_on_page = 0

                for a_tag in product_links:
                    nom = clean_name(a_tag.get_text())
                    if not nom or nom in seen_on_page:
                        continue

                    h2_tag = a_tag.find_parent("h2")
                    price_text = extract_price_from_h2(h2_tag)
                    prix = clean_price(price_text)

                    if prix is None or prix < 100:
                        continue

                    href = a_tag.get("href", "")
                    url_produit = f"https://sakanal.sn{href}" if href.startswith("/") else href
                    is_new_url = (nom, href.strip()) not in seen_signatures_in_category

                    all_data.append({
                        "nom_produit": nom,
                        "categorie": cat["categorie"],
                        "prix": prix,
                        "magasin": "Sakanal",
                        "date_scraping": date_today,
                        "url_produit": url_produit,
                    })

                    seen_on_page.add(nom)
                    products_found += 1
                    if is_new_url:
                        new_urls_on_page += 1

                print(
                    f"   {products_found} produits valides ajoutes "
                    f"(sur {total_elements} elements trouves) | "
                    f"page servie = {served_page} | page suivante = {next_page_exists} | "
                    f"recouvrement = {overlap_ratio:.0%} | nouveaux = {new_urls_on_page}"
                )

                if total_elements == 0:
                    print("   -> Fin de categorie detectee (page vide)")
                    break

                if served_page != page:
                    print(
                        f"   -> Fin de categorie detectee "
                        f"(la page demandee {page} renvoie en fait la page {served_page})"
                    )
                    break

                if previous_signature is not None and page_signature == previous_signature:
                    print("   -> Fin de categorie detectee (page fantome : meme contenu que la precedente)")
                    break

                if overlap_ratio >= 0.8:
                    print(
                        "   -> Fin de categorie detectee "
                        "(page fantome : la majorite des produits ont deja ete vus)"
                    )
                    break

                if new_urls_on_page == 0 and page > 1:
                    print("   -> Fin de categorie detectee (aucun nouveau produit sur cette page)")
                    break

                if not next_page_exists:
                    print("   -> Fin de categorie detectee (plus de page suivante)")
                    break

                previous_signature = page_signature
                seen_signatures_in_category.update(page_signature)

                sleep_time = random.uniform(1.5, 2.5)
                print(f"   Pause {sleep_time:.1f}s...")
                time.sleep(sleep_time)

            except Exception as e:
                print(f"   Erreur page {page} : {e}")
                if "timeout" in str(e).lower() or "connection" in str(e).lower():
                    print("   -> Erreur de connexion detectee, pause avant retry")
                    time.sleep(5)
                    continue  # Retry la meme page au lieu de break
                else:
                    break  # Pour les autres erreurs, on arrete la categorie

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=["nom_produit", "prix", "magasin"], keep="first")

        filename = RAW_DATA_DIR / f"donnees_brutes_sakanal_{date_today}.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")

        print(f"\nSAKANAL TERMINE -> {len(df)} lignes ({filename.name})")
        print(df.head())
    else:
        print("Aucun produit recupere.")


if __name__ == "__main__":
    scrape_sakanal()
