import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
}

CATEGORIES = [
    {"url": "https://sakanal.sn/fr/111-epicerie", "categorie": "Epicerie"},
    # Ajoute ici d'autres catégories quand tu veux :
     {"url": "https://sakanal.sn/fr/83-produits-locaux", "categorie": "Produits Locaux"},
]

MAX_PAGES_PER_CATEGORY = 30

def clean_price(price_text):
    """Nettoie le prix → retourne float ou None"""
    if not price_text:
        return None
    # Garde uniquement les chiffres
    numbers = re.sub(r'[^0-9]', '', price_text)
    try:
        return float(numbers)
    except ValueError:
        return None

def clean_name(name):
    """Nettoyage minimal pour standardisation future"""
    if not name:
        return ""
    name = re.sub(r'\s+', ' ', name.strip())
    return name  # On enlève .title() pour garder l'original

def extract_quantity_and_unit(name):
    """Bonus : essaie d'extraire la quantité (ex: 500G, 12*37CL) pour plus tard"""
    # On garde le nom tel quel pour l'instant, mais on pourra l'utiliser après
    return name  # pour le moment on ne change rien

def extract_price_from_h2(h2_tag):
    """Meilleure façon de trouver le prix juste après le h2"""
    if not h2_tag:
        return None
    
    # On cherche le prochain texte qui contient "FCFA"
    current = h2_tag.next_sibling
    for _ in range(15):  # limite raisonnable
        if current:
            text = current.get_text(strip=True) if hasattr(current, 'get_text') else str(current)
            if "FCFA" in text:
                return text
        else:
            break
        current = current.next_sibling
    return None

def scrape_sakanal():
    all_data = []
    date_today = datetime.now().strftime("%Y-%m-%d")
    
    for cat in CATEGORIES:
        print(f"\n🚀 Début du scraping → Catégorie : {cat['categorie']}")
        base_url = cat["url"]
        
        for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
            url = f"{base_url}?page={page}" if page > 1 else base_url
            print(f"   🔄 Page {page} → {url}")
            
            try:
                response = requests.get(url, headers=HEADERS, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Sélecteur principal : tous les liens de produits dans les h2
                product_links = soup.select("h2 a")
                
                products_found = 0
                seen = set()  # évite doublons sur la même page
                
                for a_tag in product_links:
                    nom = clean_name(a_tag.get_text())
                    if not nom or nom in seen:
                        continue
                    
                    h2_tag = a_tag.find_parent("h2")
                    price_text = extract_price_from_h2(h2_tag)
                    prix = clean_price(price_text)
                    
                    if prix is None or prix < 100:   # filtre les prix absurdes
                        continue
                    
                    all_data.append({
                        "nom_produit": nom,
                        "categorie": cat["categorie"],
                        "prix": prix,
                        "magasin": "Sakanal",
                        "date_scraping": date_today,
                        "url_produit": "https://sakanal.sn" + a_tag["href"] if a_tag.get("href", "").startswith("/") else a_tag.get("href", "")
                    })
                    
                    seen.add(nom)
                    products_found += 1
                
                print(f"   ✅ {products_found} produits valides ajoutés")
                
                # Arrêt intelligent
                if products_found < 25 and page > 5:
                    print("   → Fin de catégorie détectée (peu de produits sur cette page)")
                    break
                
                time.sleep(1.8)
                
            except requests.exceptions.HTTPError as e:
                print(f"   ❌ Erreur HTTP {e.response.status_code} → arrêt de la catégorie")
                break
            except Exception as e:
                print(f"   ❌ Erreur inattendue page {page} : {e}")
                break
    
    # ====================== Sauvegarde finale ======================
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=["nom_produit", "prix", "magasin"], keep='first')
        
        filename = f"donnees_brutes_sakanal_{date_today}.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        print(f"\n🎉 SCRAPING TERMINÉ AVEC SUCCÈS !")
        print(f"   Total lignes uniques : {len(df)}")
        print(f"   Fichier sauvegardé : {filename}")
        print("\nAperçu des 5 premières lignes :")
        print(df.head())
        print(f"\nColonnes : {list(df.columns)}")
    else:
        print("⚠️ Aucun produit récupéré.")

if __name__ == "__main__":
    scrape_sakanal()