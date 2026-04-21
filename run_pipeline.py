import subprocess
import sys
import time
from pathlib import Path

from project_paths import KPI_DATA_DIR, ensure_data_directories, find_latest_file


BASE_DIR = Path(__file__).resolve().parent
STEPS = [
    "scraper_auchan.py",
    "scraper_sakanal.py",
    "scraper_diarle.py",
    "fusionner_donnees.py",
    "nettoyage.py",
    "kpi.py",
]

EXPECTED_KPI_OUTPUTS = [
    "kpi_indice_variation_prix_*.csv",
    "kpi_magasin_competitif_*.csv",
    "kpi_fluctuation_produits_*.csv",
    "kpi_inflation_mensuelle_categorie_*.csv",
    "kpi_inflation_mensuelle_globale_*.csv",
    "kpi_score_categorie_magasin_*.csv",
    "kpi_contexte_ia_*.csv",
    "kpi_contexte_ia_*.json",
    "kpi_resume_*.csv",
    "kpi_panel_journalier_*.csv",
    "kpi_qualite_donnees_*.csv",
]


def run_step(script_name: str) -> float:
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Script introuvable: {script_path}")
    print(f"\n=== Execution : {script_name} ===")
    start = time.perf_counter()
    subprocess.run([sys.executable, str(script_path)], check=True)
    elapsed = time.perf_counter() - start
    print(f"--- Termine : {script_name} ({elapsed:.1f}s)")
    return elapsed


def validate_kpi_outputs() -> None:
    print("\n=== Verification des exports KPI ===")
    missing = []
    for pattern in EXPECTED_KPI_OUTPUTS:
        latest = find_latest_file(KPI_DATA_DIR, pattern)
        if latest is None:
            missing.append(pattern)
            continue
        print(f"[OK] {pattern} -> {latest.name}")
    if missing:
        missing_text = ", ".join(missing)
        raise RuntimeError(f"Fichiers KPI manquants: {missing_text}")


def main() -> None:
    ensure_data_directories()
    global_start = time.perf_counter()
    durations = []
    for script_name in STEPS:
        durations.append((script_name, run_step(script_name)))

    validate_kpi_outputs()

    total = time.perf_counter() - global_start
    print("\n=== Recap pipeline ===")
    for script_name, elapsed in durations:
        print(f"- {script_name}: {elapsed:.1f}s")
    print(f"\nPipeline termine avec succes en {total:.1f}s.")


if __name__ == "__main__":
    main()
