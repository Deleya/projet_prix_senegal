import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
import random
from fake_useragent import UserAgent

ua = UserAgent()

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer": "https://www.auchan.sn/",
    "DNT": "1",
}

CATEGORIES = [
    {"url": "https://www.auchan.sn/104-epicerie-salee", "categorie": "Epicerie Salée", "stop_threshold": 10},
    {"url": "https://www.auchan.sn/91-epicerie-sucree", "categorie": "Epicerie Sucrée", "stop_threshold": 10},
    {"url": "https://www.auchan.sn/137-boissons", "categorie": "Boissons", "stop_threshold": 7},
    {"url": "https://www.auchan.sn/92-fruits-legumes", "categorie": "Fruits & Légumes", "stop_threshold": 8},
    {"url": "https://www.auchan.sn/232-produits-surgeles", "categorie": "Produits Surgelés", "stop_threshold": 8},
    {"url": "https://www.auchan.sn/121-produits-frais", "categorie": "Produits Frais", "stop_threshold": 8},
]

MAX_PAGES_PER_CATEGORY = 50

def clean_price(price_text):
    """Nettoyage intelligent : prend UNIQUEMENT le vrai prix avant CFA/FCFA"""
    if not price_text:
        return None
    match = re.search(r'(\d[\d\s.,]*?)\s*(?:CFA|FCFA)', str(price_text), re.IGNORECASE)
    if match:
        number_str = re.sub(r'[\s.,]', '', match.group(1))
        try:
            return float(number_str)
        except ValueError:
            return None
    return None

def clean_name(name):
    if not name:
        return ""
    name = re.sub(r'\s+', ' ', name.strip())
    return name

def extract_price_from_h2(h2_tag):
    if not h2_tag:
        return None
    current = h2_tag.next_sibling
    for _ in range(25):
        if current:
            text = current.get_text(strip=True) if hasattr(current, 'get_text') else str(current)
            if "CFA" in text.upper() and "/ l" not in text and "/ kg" not in text and "/ pièce" not in text:
                return text
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
                
                if products_found == 0 and page > 2 or (products_found < threshold and page > 3):
                    print("   → Fin de catégorie détectée")
                    break
                
                sleep_time = random.uniform(7, 13)
                print(f"   ⏳ Pause {sleep_time:.1f}s (anti-Cloudflare)...")
                time.sleep(sleep_time)
                
            except Exception as e:
                print(f"   ❌ Erreur page {page} : {e}")
                break
    
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=["nom_produit", "prix", "magasin"], keep='first')
        
        filename = f"donnees_brutes_auchan_{date_today}.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        print(f"\n🎉 AUCHAN TERMINÉ → {len(df)} lignes (prix corrigés !)")
        print(df.head())

if __name__ == "__main__":
    scrape_auchan()