from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
KPI_DATA_DIR = DATA_DIR / "kpi"


def ensure_data_directories() -> None:
    for path in (RAW_DATA_DIR, PROCESSED_DATA_DIR, KPI_DATA_DIR):
        path.mkdir(parents=True, exist_ok=True)


def find_latest_file(directory: Path, pattern: str) -> Path | None:
    files = [
        path for path in directory.glob(pattern)
        if "_progress" not in path.stem
    ]
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None
