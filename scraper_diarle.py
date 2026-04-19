import random
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
from project_paths import RAW_DATA_DIR, ensure_data_directories

CATEGORIES = [
    {"url": "https://diarle.sn/fr/3-petit-dejeuner", "categorie": "Epicerie"},
    {"url": "https://diarle.sn/fr/16-epice-bouillon-condiment", "categorie": "Epicerie"},
    {"url": "https://diarle.sn/fr/44-fruits", "categorie": "Fruits & Legumes"},
    {"url": "https://diarle.sn/fr/45-legumes", "categorie": "Fruits & Legumes"},
    {"url": "https://diarle.sn/fr/14-d-p-h", "categorie": "Hygiene & Beaute"},
    {"url": "https://diarle.sn/fr/10-boissons", "categorie": "Boissons"},
    {"url": "https://diarle.sn/fr/17-produits-locaux", "categorie": "Produits Locaux"},
    {"url": "https://diarle.sn/fr/72-surgelees-charcuterie", "categorie": "Produits frais "},
    {"url": "https://sakanal.sn/fr/63-bebe-puericulture", "categorie": "Bebe & Puériculture"},
]


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


def safe_goto(page, url, timeout=45000, max_retries=2):
    """
    Sur certains sites e-commerce, networkidle n'arrive jamais vraiment.
    On charge d'abord le DOM, puis on attend un peu le reseau sans bloquer tout le script.
    Avec retry automatique pour les erreurs de connexion.
    """
    for attempt in range(max_retries + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except PlaywrightTimeoutError:
                print("   -> networkidle non atteint, on continue")
            return  # Success
        except Exception as e:
            if attempt < max_retries:
                wait_time = 5 * (attempt + 1)  # 5s, 10s
                print(f"   -> Tentative {attempt + 1} echouee ({e}), retry dans {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise  # Re-raise after all retries


def fermer_popups_si_present(page):
    selectors = [
        "button:has-text('Accepter')",
        "button:has-text(\"J'accepte\")",
        "button:has-text('OK')",
        "button:has-text('Fermer')",
        ".close",
        ".btn-close",
    ]

    for selector in selectors:
        try:
            bouton = page.locator(selector).first
            if bouton.is_visible(timeout=1000):
                bouton.click(timeout=1000)
                print(f"   -> Popup fermee via {selector}")
                time.sleep(1)
                break
        except Exception:
            continue


def extract_products_from_page(page):
    """
    Extraction en bloc dans le navigateur.
    C'est beaucoup plus fiable que 594 appels Python -> Playwright -> DOM.
    """
    return page.eval_on_selector_all(
        "article.product-miniature, .product-item, .js-product-miniature",
        """
        (cards) => cards.map((card) => {
            const nameEl = card.querySelector(
                'h2 a, h3 a, h3, .product-name, .product-title, .thumbnail-top a, a.product-thumbnail'
            );
            const priceEl = card.querySelector('span.price, .price, .product-price');
            const linkEl = card.querySelector('a[href]');
            const imgEl = card.querySelector('img[alt], img[title]');

            let nom = '';
            if (nameEl) {
                nom =
                    nameEl.innerText ||
                    nameEl.textContent ||
                    nameEl.getAttribute('title') ||
                    nameEl.getAttribute('aria-label') ||
                    '';
            }

            if (!nom && linkEl) {
                nom =
                    linkEl.getAttribute('title') ||
                    linkEl.getAttribute('aria-label') ||
                    linkEl.textContent ||
                    '';
            }

            if (!nom && imgEl) {
                nom = imgEl.getAttribute('alt') || imgEl.getAttribute('title') || '';
            }

            return {
                nom: nom,
                prix_text: priceEl ? priceEl.innerText : '',
                href: linkEl ? linkEl.getAttribute('href') : '',
                card_text: card.innerText || ''
            };
        })
        """,
    )


def save_snapshot(all_data, date_today, suffix=""):
    if not all_data:
        return None

    df = pd.DataFrame(all_data)
    df = df.drop_duplicates(subset=["nom_produit", "prix", "magasin"], keep="first")

    filename = RAW_DATA_DIR / f"donnees_brutes_diarle_{date_today}{suffix}.csv"
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    return Path(filename)


def scroll_until_stable(page, max_scrolls=40, pause_seconds=2.0):
    """
    Scroll plus agressif :
    - on s'arrete vite si le nombre de cartes n'augmente plus
    - on n'attend pas uniquement une hauteur identique
    """
    previous_height = 0
    previous_count = 0
    stable_rounds = 0
    no_growth_rounds = 0

    for scroll_attempt in range(1, max_scrolls + 1):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(pause_seconds)

        current_height = page.evaluate("document.body.scrollHeight")
        current_count = page.locator(
            "article.product-miniature, .product-item, .js-product-miniature"
        ).count()

        growth = current_count - previous_count
        print(
            f"   Scroll {scroll_attempt} | Produits visibles : {current_count} | "
            f"Hauteur : {current_height} | Gain : {growth:+d}"
        )

        if current_height == previous_height and current_count == previous_count and current_count > 0:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if growth <= 0 and current_count > 0:
            no_growth_rounds += 1
        else:
            no_growth_rounds = 0

        if stable_rounds >= 1:
            print("   -> Scroll arrete : page stable")
            break

        if no_growth_rounds >= 2:
            print("   -> Scroll arrete : plus aucune nouvelle carte visible")
            break

        previous_height = current_height
        previous_count = current_count


def scrape_diarle():
    ensure_data_directories()
    all_data = []
    date_today = datetime.now().strftime("%Y-%m-%d")
    snapshot_path = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.set_extra_http_headers({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            )
        })

        for cat in CATEGORIES:
            try:
                print(f"\nDebut du scraping Diarle -> Categorie : {cat['categorie']}")
                url = cat["url"]
                safe_goto(page, url)
                time.sleep(4)
                fermer_popups_si_present(page)
                scroll_until_stable(page, max_scrolls=40, pause_seconds=2.0)

                raw_products = extract_products_from_page(page)
                print(f"   -> Extraction finale sur {len(raw_products)} cartes")

                products_found = 0
                seen_on_page = set()
                debug_examples = []

                for item in raw_products:
                    nom = clean_name(item.get("nom", ""))
                    prix_text = item.get("prix_text", "") or item.get("card_text", "")
                    prix = clean_price(prix_text)

                    if not nom or nom in seen_on_page:
                        if len(debug_examples) < 5:
                            debug_examples.append({
                                "nom": nom,
                                "prix_text": prix_text[:120],
                                "raison": "nom vide ou duplique",
                            })
                        continue

                    if prix is None or prix < 100:
                        if len(debug_examples) < 5:
                            debug_examples.append({
                                "nom": nom,
                                "prix_text": prix_text[:120],
                                "raison": "prix introuvable ou < 100",
                            })
                        continue

                    url_produit = item.get("href", "") or ""
                    if url_produit and not url_produit.startswith("http"):
                        url_produit = f"https://diarle.sn{url_produit}"

                    all_data.append({
                        "nom_produit": nom,
                        "categorie": cat["categorie"],
                        "prix": prix,
                        "magasin": "Diarle",
                        "date_scraping": date_today,
                        "url_produit": url_produit,
                    })
                    seen_on_page.add(nom)
                    products_found += 1

                print(
                    f"   {products_found} produits valides ajoutes pour "
                    f"{cat['categorie']}"
                )
                if products_found == 0 and debug_examples:
                    print("   -> Debug extraction (exemples) :")
                    for example in debug_examples:
                        print(
                            f"      nom='{example['nom']}' | "
                            f"raison={example['raison']} | "
                            f"texte='{example['prix_text']}'"
                        )
                snapshot_path = save_snapshot(all_data, date_today, suffix="_progress")
                if snapshot_path is not None:
                    print(f"   -> Sauvegarde intermediaire : {snapshot_path.name}")
                time.sleep(random.uniform(3, 6))

            except PlaywrightTimeoutError as e:
                print(f"   Erreur timeout sur la categorie {cat['categorie']} : {e}")
                snapshot_path = save_snapshot(all_data, date_today, suffix="_progress")
                if snapshot_path is not None:
                    print(f"   -> Sauvegarde intermediaire apres erreur : {snapshot_path.name}")
                continue
            except Exception as e:
                print(f"   Erreur sur la categorie {cat['categorie']} : {e}")
                if "ERR_CONNECTION_TIMED_OUT" in str(e) or "net::" in str(e) or "timeout" in str(e).lower():
                    print("   -> Erreur de connexion reseau detectee, pause supplementaire avant prochaine categorie")
                    time.sleep(10)  # Pause plus longue pour les erreurs reseau
                snapshot_path = save_snapshot(all_data, date_today, suffix="_progress")
                if snapshot_path is not None:
                    print(f"   -> Sauvegarde intermediaire apres erreur : {snapshot_path.name}")
                continue

        browser.close()

    final_path = save_snapshot(all_data, date_today)
    if final_path is not None:
        df = pd.read_csv(final_path)
        print(f"\nDIARLE TERMINE -> {len(df)} lignes")
        print(df.head())
    else:
        print("Aucun produit recupere.")


if __name__ == "__main__":
    scrape_diarle()
