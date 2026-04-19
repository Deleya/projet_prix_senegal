# Projet Prix Senegal

Projet d'analyse des prix de produits de consommation au Senegal a partir de plusieurs sites e-commerce.

## Objectif

Le projet collecte les prix depuis plusieurs enseignes, nettoie et standardise les produits, calcule des KPI metier, puis affiche les resultats dans un dashboard Streamlit.

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

## Historique

Le projet construit un historique a partir des fichiers `donnees_analytiques_kpi_*.csv` generes a des dates differentes.
Plus tu relances le pipeline sur plusieurs jours, plus les KPI de variation, inflation et fluctuation deviennent pertinents.

## Etat actuel

- scraping multi-sources: en place
- comparabilite produits: fiabilisee
- KPI: fiabilises sur `comparable_normalise`
- dashboard: aligne sur les filtres et la qualite
- automatisation: point d'entree `run_pipeline.py` pret pour une planification locale
