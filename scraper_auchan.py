import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
import random
from fake_useragent import UserAgent

# ==================== CONFIGURATION ANTI-CLOUDFLARE ====================
ua = UserAgent()

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer": "https://www.auchan.sn/",
    "DNT": "1",
}

# ==================== CATÉGORIES AVEC SEUIL ADAPTÉ ====================
CATEGORIES = [
    #{"url": "https://www.auchan.sn/104-epicerie-salee", "categorie": "Epicerie Salée", "stop_threshold": 10},
    #{"url": "https://www.auchan.sn/91-epicerie-sucree", "categorie": "Epicerie Sucrée", "stop_threshold": 10},
    {"url": "https://www.auchan.sn/137-boissons", "categorie": "Boissons", "stop_threshold": 7},
    {"url": "https://www.auchan.sn/92-fruits-legumes", "categorie": "Fruits & Légumes", "stop_threshold": 8},
    # Ajoute ici d'autres catégories (ex: surgelés, frais...)
]

MAX_PAGES_PER_CATEGORY = 50

def clean_price(price_text):
    if not price_text:
        return None
    numbers = re.sub(r'[^0-9]', '', price_text)
    try:
        return float(numbers)
    except ValueError:
        return None

def clean_name(name):
    if not name:
        return ""
    name = re.sub(r'\s+', ' ', name.strip())
    return name

def extract_price_from_h2(h2_tag):
    """NOUVELLE VERSION : prend le PRIX TOTAL (celui sans '/ l' ou '/ kg')"""
    if not h2_tag:
        return None
    
    current = h2_tag.next_sibling
    for _ in range(20):  # on cherche plus loin
        if current:
            text = current.get_text(strip=True) if hasattr(current, 'get_text') else str(current)
            if "CFA" in text:
                # On ignore le prix unitaire (qui contient / l ou / kg)
                if "/ l" not in text and "/ kg" not in text and "/ pièce" not in text:
                    return text  # ← c'est le prix total !
        else:
            break
        current = current.next_sibling
    return None

def scrape_auchan():
    all_data = []
    date_today = datetime.now().strftime("%Y-%m-%d")
    
    session = requests.Session()
    
    for cat in CATEGORIES:
        threshold = cat.get("stop_threshold", 10)
        print(f"\n🚀 Début du scraping → Catégorie : {cat['categorie']} (seuil = {threshold})")
        base_url = cat["url"]
        
        for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
            url = f"{base_url}?page={page}" if page > 1 else base_url
            print(f"   🔄 Page {page} → {url}")
            
            headers = HEADERS_BASE.copy()
            headers["User-Agent"] = ua.random
            
            try:
                response = session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, "html.parser")
                product_links = soup.select("h2 a")
                
                products_found = 0
                seen = set()
                
                for a_tag in product_links:
                    nom = clean_name(a_tag.get_text())
                    if not nom or nom in seen:
                        continue
                    
                    h2_tag = a_tag.find_parent("h2")
                    price_text = extract_price_from_h2(h2_tag)
                    prix = clean_price(price_text)
                    
                    if prix is None or prix < 100:
                        continue
                    
                    all_data.append({
                        "nom_produit": nom,
                        "categorie": cat["categorie"],
                        "prix": prix,
                        "magasin": "Auchan",
                        "date_scraping": date_today,
                        "url_produit": "https://www.auchan.sn" + a_tag["href"] 
                                        if a_tag.get("href", "").startswith("/") 
                                        else a_tag.get("href", "")
                    })
                    
                    seen.add(nom)
                    products_found += 1
                
                print(f"   ✅ {products_found} produits valides ajoutés")
                
                if products_found < threshold and page > 5:
                    print(f"   → Fin de catégorie détectée (seuil {threshold} atteint)")
                    break
                
                sleep_time = random.uniform(6, 12)
                print(f"   ⏳ Pause {sleep_time:.1f}s (anti-Cloudflare)...")
                time.sleep(sleep_time)
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 522:
                    print("   ❌ Erreur 522 Cloudflare → catégorie suivante")
                    break
                else:
                    print(f"   ❌ Erreur HTTP {e.response.status_code}")
                    break
            except Exception as e:
                print(f"   ❌ Erreur page {page} : {e}")
                break
    
    # ====================== Sauvegarde ======================
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=["nom_produit", "prix", "magasin"], keep='first')
        
        filename = f"donnees_brutes_auchan_{date_today}.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        print(f"\n🎉 SCRAPING AUCHAN TERMINÉ !")
        print(f"   Total lignes uniques : {len(df)}")
        print(f"   Fichier : {filename}")
        print("\nAperçu :")
        print(df.head())
    else:
        print("⚠️ Aucun produit récupéré.")

if __name__ == "__main__":
    scrape_auchan()