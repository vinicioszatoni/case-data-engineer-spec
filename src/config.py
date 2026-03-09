from dotenv import load_dotenv
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

CHAVE_API = os.getenv("CHAVE_API")

COINCAP_BASE_URL = os.getenv("COINCAP_BASE_URL")
COINCAP_ASSET_IDS = [
    asset.strip() for asset in os.getenv("COINCAP_ASSET_IDS", "").split(",") if asset.strip()
]
COINCAP_SEARCH = os.getenv("COINCAP_SEARCH", "")
COINCAP_LIMIT = int(os.getenv("COINCAP_LIMIT", "100"))
COINCAP_OFFSET = int(os.getenv("COINCAP_OFFSET", "0"))

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW = os.getenv("GCS_RAW", "raw/coincap/assets")
GCS_PROCESSED = os.getenv("GCS_PROCESSED", "processed/coincap/assets")

BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")