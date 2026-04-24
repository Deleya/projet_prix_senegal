# Projet Prix Senegal

Projet d'analyse des prix de produits de consommation au Senegal a partir de plusieurs sites e-commerce.

## Objectif

Le projet collecte les prix depuis plusieurs enseignes, nettoie et standardise les produits, calcule des KPI metier, puis affiche les resultats dans un dashboard Streamlit.

L'approche s'appuie sur **3 niveaux de preuve**:

- **Niveau 1 (strict)**: comparaison produit-a-produit uniquement quand le matching est fiable.
- **Niveau 2 (macro)**: comparaison par **categorie x magasin** (positionnement prix + dispersion + couverture).
- **Niveau 3 (qualite/couverture)**: toujours afficher la couverture et un niveau de confiance pour limiter les biais d'assortiment.

## Structure du repo

```text
.
|-- app.py
|-- run_pipeline.py
|-- scraper_auchan.py
|-- scraper_diarle.py
|-- scraper_sakanal.py
|-- fusionner_donnees.py
|-- nettoyage.py
|-- kpi.py
|-- project_paths.py
|-- requirements.txt
|-- data/
|   |-- raw/
|   |-- processed/
|   `-- kpi/
`-- docs/
```

## Pipeline

Le pipeline suit cet ordre:

1. `scraper_auchan.py`
2. `scraper_sakanal.py`
3. `scraper_diarle.py`
4. `fusionner_donnees.py`
5. `nettoyage.py`
6. `kpi.py`

Les donnees generees sont rangees dans:

- `data/raw`: sorties de scraping et donnees fusionnees
- `data/processed`: donnees analytiques nettoyees et referentiels
- `data/kpi`: exports KPI et panel journalier

## Exports KPI (data/kpi)

Les fichiers suivants sont regeneres a chaque execution de `kpi.py` (suffixe date):

- `kpi_score_categorie_magasin_<date>.csv` (**Niveau 2**): tableau `categorie_standardisee x magasin_standardise` avec:
  - `nb_produits`, `prix_median`, `prix_min`, `prix_max`
  - `couverture_categorie_pct`
  - `indice_categorie_base_100` (base 100 par categorie pour comparer les positionnements prix)
- `kpi_contexte_ia_<date>.csv` et `kpi_contexte_ia_<date>.json`: vue "LLM-ready" avec `score_confiance` + `note_methodologique` (pret a injecter dans un futur assistant).
- `kpi_panel_journalier_<date>.csv`: panel journalier des produits comparables (base du Niveau 1).
- `kpi_magasin_competitif_<date>.csv`: competitivite magasin sur les produits strictement comparables.
- `kpi_indice_variation_prix_<date>.csv`: indice d'evolution des prix (base 100 dans le temps).
- `kpi_inflation_mensuelle_categorie_<date>.csv`, `kpi_inflation_mensuelle_globale_<date>.csv`: tendances d'inflation.
- `kpi_fluctuation_produits_<date>.csv`: produits les plus volatils.
- `kpi_qualite_donnees_<date>.csv`: rapport de qualite (taux de retention, cles comparables, etc.).
- `kpi_resume_<date>.csv`: resume court des KPI.

## Installation

```powershell
py -3.13 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Execution manuelle

### Pipeline complet

```powershell
python .\run_pipeline.py
```

### Dashboard

```powershell
streamlit run .\app.py
```

Le dashboard charge automatiquement les **derniers exports** disponibles dans `data/kpi/` et les affiche dans:

- une **heatmap** de positionnement (`indice_categorie_base_100`)
- un boxplot de **dispersion des prix** (par categorie, comparaison entre magasins)
- une vue **Contexte IA** (donnees injectables + score de confiance)

## Assistant IA (Groq)

Le dashboard inclut un assistant conversationnel:

- moteur LLM Groq (mode principal: `llama-3.1-70b-versatile`, fallback: `llama-3.1-8b-instant`)
- bouton assistant flottant (popover) pour discuter sans quitter la page
- reponses contraintes au contexte du projet (`kpi_contexte_ia_*`) avec garde-fous anti-hallucination

Configuration cle API:

- via variable d'environnement `GROQ_API_KEY`
- ou via fichier `.env` a la racine:

```text
GROQ_API_KEY=ta_cle
```

Le fichier `.env` est ignore par git.

## Historique

Le projet construit un historique a partir des fichiers `donnees_analytiques_kpi_*.csv` generes a des dates differentes.
Plus tu relances le pipeline sur plusieurs jours, plus les KPI de variation, inflation et fluctuation deviennent pertinents.

## Etat actuel

- scraping multi-sources: en place
- comparabilite produits: fiabilisee
- KPI: fiabilises sur `comparable_normalise`
- dashboard: vues Niveau 1 + Niveau 2 + Contexte IA + Assistant Groq
- automatisation: point d'entree `run_pipeline.py` (execution + verification des exports attendus)
