import logging
from google.cloud import bigquery

from config import GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def carregar_parquet_no_bigquery(gcs_uri: str) -> str:
    if not BQ_DATASET or not BQ_TABLE:
        raise ValueError("BQ_DATASET ou BQ_TABLE não carregados do .env")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    logging.info(f"Carga concluída no BigQuery: {table_id} <- {gcs_uri}")
    return table_id