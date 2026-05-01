# 🇸🇳 Projet Prix Sénégal | Senegal Price Analysis Project

**[🇫🇷 VERSION FRANÇAISE](#version-française) • [🇬🇧 ENGLISH VERSION](#english-version)**

---

## Version Française

# 📊 Projet Prix Sénégal

Une plateforme complète d'analyse des prix de produits de consommation au Sénégal, collectée depuis plusieurs sites e-commerce majeurs (Auchan, Diarle, Sakanal) avec KPI métier, dashboard interactif et assistant IA intelligent.

## 🎯 Objectif

Analyser les prix et la compétitivité des produits de consommation au Sénégal en agrégeant les données de plusieurs enseignes, nettoyant et standardisant les produits, calculant des KPI métier pertinents, puis affichant les résultats dans un dashboard Streamlit interactif avec assistant IA.

### 3 Niveaux de Preuve

- **Niveau 1 (Strict)** : Comparaison produit-à-produit uniquement quand le matching est fiable
- **Niveau 2 (Macro)** : Comparaison par catégorie × magasin (positionnement prix, dispersion, couverture)
- **Niveau 3 (Qualité/Couverture)** : Affichage systématique de la couverture et du niveau de confiance pour limiter les biais d'assortiment

## 📁 Structure du Dépôt

```
projet_prix_senegal/
├── app.py                      # Dashboard Streamlit
├── run_pipeline.py             # Point d'entrée du pipeline
├── project_paths.py            # Gestion des chemins
│
├── Scrapers (données brutes)
│   ├── scraper_auchan.py
│   ├── scraper_diarle.py
│   └── scraper_sakanal.py
│
├── Pipeline de traitement
│   ├── fusionner_donnees.py    # Fusion des sources
│   ├── nettoyage.py            # Standardisation et nettoyage
│   └── kpi.py                  # Calcul des KPI
│
├── data/
│   ├── raw/                    # Sorties scraping et fusion
│   ├── processed/              # Données analytiques nettoyées
│   └── kpi/                    # Exports KPI
│
├── docs/                       # Documentation supplémentaire
├── requirements.txt            # Dépendances Python
└── .env                        # Configuration (ignoré par git)
```

## 🔄 Pipeline de Données

Le pipeline suit cet ordre strict :

1. **Scraping** : Auchan → Sakanal → Diarle
2. **Fusion** : Consolidation des 3 sources
3. **Nettoyage** : Standardisation des produits et prix
4. **KPI** : Calcul des métriques métier

### Sorties du Pipeline

| Fichier | Description | Fréquence |
|---------|-------------|-----------|
| `kpi_score_categorie_magasin_*.csv` | Matrice catégorie × magasin (Niveau 2) | À chaque exécution |
| `kpi_contexte_ia_*.csv/.json` | Données prêtes pour LLM + score de confiance | À chaque exécution |
| `kpi_panel_journalier_*.csv` | Produits strictement comparables (Niveau 1) | À chaque exécution |
| `kpi_magasin_competitif_*.csv` | Compétitivité magasin | À chaque exécution |
| `kpi_indice_variation_prix_*.csv` | Indice d'évolution des prix (base 100) | Historique |
| `kpi_inflation_mensuelle_*.csv` | Tendances inflation catégorie/globale | Historique |
| `kpi_fluctuation_produits_*.csv` | Produits les plus volatils | Historique |
| `kpi_qualite_donnees_*.csv` | Rapport qualité données | À chaque exécution |
| `kpi_resume_*.csv` | Résumé court des KPI | À chaque exécution |

## 🛠️ Installation

### Prérequis
- Python 3.13+
- pip

### Étapes d'installation

```bash
# 1. Créer l'environnement virtuel
python -m venv venv

# 2. Activer l'environnement
# Windows:
.\venv\Scripts\Activate.ps1
# Linux/Mac:
source venv/bin/activate

# 3. Mettre à jour pip
python -m pip install --upgrade pip

# 4. Installer les dépendances
pip install -r requirements.txt
```

## 🚀 Utilisation

### Exécuter le pipeline complet

```bash
python run_pipeline.py
```

Cette commande exécute automatiquement :
1. Les 3 scrapers
2. La fusion des données
3. Le nettoyage
4. Le calcul des KPI
5. Vérifie les exports attendus

### Lancer le dashboard

```bash
streamlit run app.py
```

Puis ouvrez `http://localhost:8501` dans votre navigateur.

#### Fonctionnalités du Dashboard

- **Heatmap de positionnement** : Indice catégorie (base 100) par magasin
- **Boxplot de dispersion** : Distribution des prix par catégorie et magasin
- **Vue Contexte IA** : Données prêtes à injecter dans un LLM + score de confiance
- **Assistant IA flottant** : Discutez des données en temps réel

## 🤖 Assistant IA (Groq)

Le dashboard intègre un assistant conversationnel alimenté par Groq :

### Configuration

L'API Groq doit être configurée via :

**Option 1 : Variable d'environnement**
```bash
export GROQ_API_KEY=sk_xxxxxxxxxxxx
```

**Option 2 : Fichier `.env` (recommandé)**
```
GROQ_API_KEY=sk_xxxxxxxxxxxx
```

⚠️ **Important** : Le fichier `.env` est ignoré par git (sécurité)

### Modèles utilisés

- **Principal** : `llama-3.1-70b-versatile` (performance maximum)
- **Fallback** : `llama-3.1-8b-instant` (si le modèle principal indisponible)

### Utilisation

1. Cliquez sur le bouton assistant flottant (coin bas droit)
2. Posez des questions sur les données KPI
3. L'assistant analyse le contexte KPI et répond (avec garde-fous anti-hallucination)
4. Les réponses restent dans le contexte du projet

## 📈 Construction de l'Historique

Le projet crée automatiquement un historique à partir des fichiers KPI datés.

**Pour obtenir des métriques d'inflation et de fluctuation pertinentes :**
- Exécutez le pipeline régulièrement (quotidiennement ou hebdomadairement)
- L'historique s'enrichit automatiquement avec chaque exécution

## 📦 Dépendances

| Package | Usage |
|---------|-------|
| `pandas` | Manipulation de données |
| `streamlit` | Dashboard web |
| `beautifulsoup4` | Parsing HTML |
| `selenium` | Web scraping dynamique |
| `requests` | Requêtes HTTP |
| `fake-useragent` | User-agents aléatoires |
| `groq` | API LLM Groq |
| `streamlit-float` | Composants flottants |

## 🔍 KPI Détaillés (Niveau 2)

Le fichier `kpi_score_categorie_magasin_*.csv` contient :

```
categorie_standardisee      → Catégorie normalisée
magasin_standardise         → Magasin normalisé
nb_produits                 → Nombre de produits
prix_median                 → Prix médian de la catégorie
prix_min / prix_max         → Fourchette de prix
couverture_categorie_pct    → % de couverture de la catégorie
indice_categorie_base_100   → Indice de positionnement (base 100)
```

## 📊 Cas d'Usage

1. **Analyse compétitive** : Comparer positionnement prix entre magasins
2. **Surveillance d'inflation** : Tracker l'évolution mensuelle des prix
3. **Identification de produits volatils** : Détecter fluctuations anormales
4. **Évaluation de couverture** : Analyser l'assortiment par magasin/catégorie
5. **Support décisionnel** : Questions ad-hoc via l'assistant IA

## ⚙️ Configuration Avancée

### Modifier les chemins de données

Éditez `project_paths.py` pour changer les répertoires de sortie.

### Ajouter un nouveau scraper

1. Créez `scraper_nouveau.py`
2. Exportez les données en format CSV
3. Ajoutez l'appel dans `run_pipeline.py`

### Ajouter des catégories de standardisation

Modifiez la logique de nettoyage dans `nettoyage.py`.

## 📝 Formats de Sortie

### CSV
- Léger et compatible Excel
- Idéal pour dashboard et analyse

### JSON
- Format `kpi_contexte_ia_*.json` pour LLM
- Facile à consommer dans APIs

## 🐛 Dépannage

### Le dashboard ne se lance pas
```bash
# Vérifier les dépendances
pip list | grep streamlit

# Réinstaller si nécessaire
pip install --upgrade streamlit
```

### Les données KPI sont absentes
```bash
# Vérifier que le pipeline a bien exécuté
python run_pipeline.py

# Vérifier les fichiers générés
ls data/kpi/
```

### L'assistant IA ne répond pas
1. Vérifiez la clé API Groq
2. Vérifiez la connexion Internet
3. Consultez les logs Streamlit

## 📄 Licence

[À compléter selon votre licence]

## 👤 Auteur

Projet développé pour l'analyse des prix au Sénégal.

## 🤝 Contribution

Pour contribuer :
1. Fork le dépôt
2. Créez une branche (`git checkout -b feature/AmazingFeature`)
3. Commit vos changements (`git commit -m 'Add AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrez une Pull Request

---

# English Version

# 📊 Senegal Price Analysis Project

A comprehensive price analysis platform for consumer products in Senegal, collecting data from multiple e-commerce sites (Auchan, Diarle, Sakanal) with business KPIs, interactive dashboard, and intelligent AI assistant.

## 🎯 Objective

Analyze prices and product competitiveness in Senegal by aggregating data from multiple retailers, cleaning and standardizing products, calculating relevant business KPIs, and displaying results in an interactive Streamlit dashboard with AI assistant.

### 3 Levels of Evidence

- **Level 1 (Strict)** : Product-to-product comparison only when matching is reliable
- **Level 2 (Macro)** : Category × Store comparison (price positioning, dispersion, coverage)
- **Level 3 (Quality/Coverage)** : Systematic display of coverage and confidence level to limit assortment bias

## 📁 Repository Structure

```
projet_prix_senegal/
├── app.py                      # Streamlit Dashboard
├── run_pipeline.py             # Pipeline entry point
├── project_paths.py            # Path management
│
├── Scrapers (raw data)
│   ├── scraper_auchan.py
│   ├── scraper_diarle.py
│   └── scraper_sakanal.py
│
├── Processing Pipeline
│   ├── fusionner_donnees.py    # Data merging
│   ├── nettoyage.py            # Standardization & cleaning
│   └── kpi.py                  # KPI calculation
│
├── data/
│   ├── raw/                    # Scraping & merge outputs
│   ├── processed/              # Cleaned analytical data
│   └── kpi/                    # KPI exports
│
├── docs/                       # Additional documentation
├── requirements.txt            # Python dependencies
└── .env                        # Configuration (git ignored)
```

## 🔄 Data Pipeline

The pipeline follows this strict order:

1. **Scraping** : Auchan → Sakanal → Diarle
2. **Merging** : Consolidate 3 sources
3. **Cleaning** : Standardize products and prices
4. **KPI** : Calculate business metrics

### Pipeline Outputs

| File | Description | Frequency |
|------|-------------|-----------|
| `kpi_score_categorie_magasin_*.csv` | Category × Store matrix (Level 2) | Every run |
| `kpi_contexte_ia_*.csv/.json` | LLM-ready data + confidence score | Every run |
| `kpi_panel_journalier_*.csv` | Strictly comparable products (Level 1) | Every run |
| `kpi_magasin_competitif_*.csv` | Store competitiveness | Every run |
| `kpi_indice_variation_prix_*.csv` | Price evolution index (base 100) | History |
| `kpi_inflation_mensuelle_*.csv` | Monthly inflation trends | History |
| `kpi_fluctuation_produits_*.csv` | Most volatile products | History |
| `kpi_qualite_donnees_*.csv` | Data quality report | Every run |
| `kpi_resume_*.csv` | Short KPI summary | Every run |

## 🛠️ Installation

### Prerequisites
- Python 3.13+
- pip

### Installation Steps

```bash
# 1. Create virtual environment
python -m venv venv

# 2. Activate environment
# Windows:
.\venv\Scripts\Activate.ps1
# Linux/Mac:
source venv/bin/activate

# 3. Upgrade pip
python -m pip install --upgrade pip

# 4. Install dependencies
pip install -r requirements.txt
```

## 🚀 Usage

### Run complete pipeline

```bash
python run_pipeline.py
```

This command automatically executes:
1. All 3 scrapers
2. Data merging
3. Cleaning
4. KPI calculation
5. Verifies expected exports

### Launch dashboard

```bash
streamlit run app.py
```

Then open `http://localhost:8501` in your browser.

#### Dashboard Features

- **Positioning Heatmap** : Category index (base 100) by store
- **Dispersion Boxplot** : Price distribution by category and store
- **AI Context View** : Data ready for LLM injection + confidence score
- **Floating AI Assistant** : Discuss data in real-time

## 🤖 AI Assistant (Groq)

The dashboard integrates a Groq-powered conversational assistant:

### Configuration

Groq API must be configured via:

**Option 1 : Environment Variable**
```bash
export GROQ_API_KEY=sk_xxxxxxxxxxxx
```

**Option 2 : `.env` File (recommended)**
```
GROQ_API_KEY=sk_xxxxxxxxxxxx
```

⚠️ **Important** : The `.env` file is ignored by git (security)

### Models Used

- **Primary** : `llama-3.1-70b-versatile` (maximum performance)
- **Fallback** : `llama-3.1-8b-instant` (if primary unavailable)

### Usage

1. Click the floating assistant button (bottom right)
2. Ask questions about KPI data
3. Assistant analyzes KPI context and responds (with hallucination guards)
4. Responses stay within project context

## 📈 Building History

The project automatically creates history from dated KPI files.

**To get meaningful inflation and fluctuation metrics:**
- Run the pipeline regularly (daily or weekly)
- History enriches automatically with each execution

## 📦 Dependencies

| Package | Usage |
|---------|-------|
| `pandas` | Data manipulation |
| `streamlit` | Web dashboard |
| `beautifulsoup4` | HTML parsing |
| `selenium` | Dynamic web scraping |
| `requests` | HTTP requests |
| `fake-useragent` | Random user agents |
| `groq` | Groq LLM API |
| `streamlit-float` | Floating components |

## 🔍 Detailed KPIs (Level 2)

The `kpi_score_categorie_magasin_*.csv` file contains:

```
categorie_standardisee      → Normalized category
magasin_standardise         → Normalized store
nb_produits                 → Number of products
prix_median                 → Median price of category
prix_min / prix_max         → Price range
couverture_categorie_pct    → Category coverage %
indice_categorie_base_100   → Positioning index (base 100)
```

## 📊 Use Cases

1. **Competitive Analysis** : Compare price positioning between stores
2. **Inflation Monitoring** : Track monthly price evolution
3. **Volatile Products** : Detect abnormal price fluctuations
4. **Coverage Assessment** : Analyze assortment by store/category
5. **Decision Support** : Ad-hoc questions via AI assistant

## ⚙️ Advanced Configuration

### Modify data paths

Edit `project_paths.py` to change output directories.

### Add a new scraper

1. Create `scraper_nouveau.py`
2. Export data in CSV format
3. Add call in `run_pipeline.py`

### Add standardization categories

Modify cleaning logic in `nettoyage.py`.

## 📝 Output Formats

### CSV
- Lightweight and Excel compatible
- Ideal for dashboards and analysis

### JSON
- `kpi_contexte_ia_*.json` format for LLM
- Easy to consume in APIs

## 🐛 Troubleshooting

### Dashboard won't launch
```bash
# Check dependencies
pip list | grep streamlit

# Reinstall if needed
pip install --upgrade streamlit
```

### KPI data is missing
```bash
# Verify pipeline executed
python run_pipeline.py

# Check generated files
ls data/kpi/
```

### AI assistant not responding
1. Verify Groq API key
2. Check Internet connection
3. Check Streamlit logs

## 📄 License

[To be completed according to your license]

## 👤 Author

Project developed for price analysis in Senegal.

## 🤝 Contributing

To contribute:
1. Fork the repository
2. Create a branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

**Last Updated:** May 1, 2026
