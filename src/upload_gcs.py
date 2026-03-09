import logging
from pathlib import Path
from datetime import datetime
from google.cloud import storage

from config import GCS_BUCKET, GCS_RAW, GCS_PROCESSED

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def upload_arquivo_gcs(local_file_path: str, asset_id: str, base_prefix: str) -> str:
    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET não carregada do .env")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    file_name = Path(local_file_path).name
    data_particao = datetime.now().strftime("%Y/%m/%d")
    blob_path = f"{base_prefix}/{asset_id}/{data_particao}/{file_name}"

    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_file_path)

    gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"
    logging.info(f"Upload realizado com sucesso para {gcs_uri}")

    return gcs_uri


def upload_raw(local_file_path: str, asset_id: str) -> str:
    return upload_arquivo_gcs(local_file_path, asset_id, GCS_RAW)


def upload_processed(local_file_path: str, asset_id: str) -> str:
    return upload_arquivo_gcs(local_file_path, asset_id, GCS_PROCESSED)