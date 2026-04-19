import random
import re
import time
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from project_paths import RAW_DATA_DIR, ensure_data_directories

ua = UserAgent()

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer": "https://www.auchan.sn/",
    "DNT": "1",
}

CATEGORIES = [
    {"url": "https://www.auchan.sn/104-epicerie-salee", "categorie": "Epicerie"},
    {"url": "https://www.auchan.sn/91-epicerie-sucree", "categorie": "Epicerie"},
    {"url": "https://www.auchan.sn/137-boissons", "categorie": "Boissons"},
    {"url": "https://www.auchan.sn/92-fruits-legumes", "categorie": "Fruits & Legumes"},
    {"url": "https://www.auchan.sn/151-hygiene-beaute-parapharmacie", "categorie": "Hygiene & Beaute"},
    {"url": "https://www.auchan.sn/121-produits-frais", "categorie": "Produits frais "},
    {"url": "https://www.auchan.sn/122-bebe-puericulture", "categorie": "Bebe & Puériculture"},
]

MAX_PAGES_PER_CATEGORY = 50


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
        upper_text = text.upper()

        # On ignore les prix au litre / kilo / piece, qui ne sont pas le prix reel du produit.
        if "CFA" in upper_text and "/ L" not in upper_text and "/ KG" not in upper_text and "/ PIECE" not in upper_text:
            return text

        current = current.next_sibling

    return None


def extract_current_page_number(soup, response_url):
    """
    Retrouve la vraie page servie.
    Si Auchan redirige une page inexistante, on veut le voir.
    """
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
    """
    Mieux que "moins de produits = fin".
    On regarde si la pagination annonce encore une page suivante.
    """
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
    """
    Mesure combien de produits de la page courante ont deja ete vus
    dans la categorie. Tres utile contre les pages fantomes.
    """
    if not page_signature:
        return 0.0

    repeated = sum(1 for item in page_signature if item in seen_signatures)
    return repeated / len(page_signature)


def build_page_signature(product_links):
    """
    Signature de la page.
    Si une page inexistante renvoie les memes produits que la precedente,
    on detecte une page fantome.
    """
    signature = []
    for a_tag in product_links:
        name = clean_name(a_tag.get_text())
        href = a_tag.get("href", "").strip()
        if name or href:
            signature.append((name, href))
    return tuple(signature)


def extract_product_links(soup):
    """
    On tente d'abord des selecteurs plus precis.
    Le fallback "h2 a" reste la pour ne pas casser le scraper si le HTML change legerement.
    """
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


def scrape_auchan():
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

            headers = HEADERS_BASE.copy()
            headers["User-Agent"] = ua.random

            try:
                response = session.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                product_links = extract_product_links(soup)
                total_elements = len(product_links)
                page_signature = build_page_signature(product_links)
                served_page = extract_current_page_number(soup, response.url)
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

                    if prix is None or prix < 50:
                        continue

                    href = a_tag.get("href", "")
                    url_produit = f"https://www.auchan.sn{href}" if href.startswith("/") else href
                    is_new_url = (nom, href.strip()) not in seen_signatures_in_category

                    all_data.append({
                        "nom_produit": nom,
                        "categorie": cat["categorie"],
                        "prix": prix,
                        "magasin": "Auchan",
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

                # Si la page courante ne contient quasiment que des produits deja vus,
                # c'est generalement une page fantome ou une pagination qui boucle.
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

                sleep_time = random.uniform(7, 13)
                print(f"   Pause {sleep_time:.1f}s (anti-Cloudflare)...")
                time.sleep(sleep_time)

            except Exception as e:
                print(f"   Erreur page {page} : {e}")
                break

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=["nom_produit", "prix", "magasin"], keep="first")

        filename = RAW_DATA_DIR / f"donnees_brutes_auchan_{date_today}.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")

        print(f"\nAUCHAN TERMINE -> {len(df)} lignes ({filename.name})")
        print(df.head())
    else:
        print("Aucun produit recupere.")


if __name__ == "__main__":
    scrape_auchan()
