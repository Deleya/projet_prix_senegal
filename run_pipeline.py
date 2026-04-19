import subprocess
import sys
from pathlib import Path

from project_paths import ensure_data_directories


BASE_DIR = Path(__file__).resolve().parent
STEPS = [
    "scraper_auchan.py",
    "scraper_sakanal.py",
    "scraper_diarle.py",
    "fusionner_donnees.py",
    "nettoyage.py",
    "kpi.py",
]


def run_step(script_name: str) -> None:
    script_path = BASE_DIR / script_name
    print(f"\n=== Execution : {script_name} ===")
    subprocess.run([sys.executable, str(script_path)], check=True)


def main() -> None:
    ensure_data_directories()
    for script_name in STEPS:
        run_step(script_name)
    print("\nPipeline termine avec succes.") 


if __name__ == "__main__":
    main()
