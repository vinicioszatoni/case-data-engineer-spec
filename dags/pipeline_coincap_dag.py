import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow.sdk import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# CONFIGURAÇÕES
# =========================
BASE_DIR = Path("/opt/airflow")
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw" / "coincap" / "assets"
REFINED_DIR = DATA_DIR / "refined" / "coincap" / "assets"

RAW_DIR.mkdir(parents=True, exist_ok=True)
REFINED_DIR.mkdir(parents=True, exist_ok=True)

CHAVE_API = os.getenv("CHAVE_API")
COINCAP_BASE_URL = os.getenv("COINCAP_BASE_URL", "https://rest.coincap.io/v3/assets")
COINCAP_ASSET_IDS = os.getenv("COINCAP_ASSET_IDS", "")
COINCAP_SEARCH = os.getenv("COINCAP_SEARCH", "")
COINCAP_LIMIT = int(os.getenv("COINCAP_LIMIT", "100"))
COINCAP_OFFSET = int(os.getenv("COINCAP_OFFSET", "0"))

GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW = os.getenv("GCS_RAW", "raw/coincap/assets")
GCS_PROCESSED = os.getenv("GCS_PROCESSED", "processed/coincap/assets")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE", "coincap_assets")
BQ_VIEW_LATEST = os.getenv("BQ_VIEW_LATEST", "vw_coincap_assets_latest")
BQ_VIEW_HISTORY = os.getenv("BQ_VIEW_HISTORY", "vw_coincap_assets_history")

GCP_CONN_ID = os.getenv("AIRFLOW_GCP_CONN_ID", "google_cloud_default")


@dag(
    dag_id="coincap_assets_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["coincap", "gcs", "bigquery", "crypto", "etl"],
    doc_md="""
    Pipeline ETL CoinCap:
    1. Extrai dados da API
    2. Salva arquivo raw local
    3. Faz upload raw para GCS
    4. Transforma os dados para refined
    5. Faz upload refined para GCS
    6. Carrega tabela trusted no BigQuery
    7. Atualiza views analíticas no BigQuery
    """,
)
def coincap_pipeline():

    @task
    def task_1_extrai_dados_api() -> dict:
        headers = {
            "Authorization": f"Bearer {CHAVE_API}"
        }

        params = {
            "ids": COINCAP_ASSET_IDS,
            "search": COINCAP_SEARCH,
            "limit": COINCAP_LIMIT,
            "offset": COINCAP_OFFSET,
        }

        response = requests.get(
            COINCAP_BASE_URL,
            headers=headers,
            params=params,
            timeout=60
        )
        response.raise_for_status()

        payload = response.json()

        if "data" not in payload or not payload["data"]:
            raise ValueError("A API não retornou dados em 'data'.")

        return payload

    @task
    def task_2_salva_raw_local(payload: dict) -> str:
        execution_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file_path = RAW_DIR / f"coincap_assets_raw_{execution_ts}.json"

        with open(raw_file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        return str(raw_file_path)

    @task
    def task_4_transforma_refined(payload: dict) -> str:
        execution_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        refined_file_path = REFINED_DIR / f"coincap_assets_refined_{execution_ts}.csv"

        df = pd.json_normalize(payload["data"])
        df.columns = [col.strip().lower() for col in df.columns]

        numeric_cols = [
            "rank",
            "supply",
            "maxsupply",
            "marketcapusd",
            "volumeusd24hr",
            "priceusd",
            "changepercent24hr",
            "vwap24hr",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Metadados de ingestão
        df["ingestion_timestamp"] = pd.Timestamp.utcnow().isoformat()
        df["ingestion_date"] = pd.Timestamp.utcnow().date().isoformat()
        df["source"] = "coincap_api"

        # Garante colunas esperadas no BigQuery
        ordered_cols = [
            "id",
            "rank",
            "symbol",
            "name",
            "supply",
            "maxsupply",
            "marketcapusd",
            "volumeusd24hr",
            "priceusd",
            "changepercent24hr",
            "vwap24hr",
            "explorer",
            "ingestion_timestamp",
            "ingestion_date",
            "source",
        ]

        for col in ordered_cols:
            if col not in df.columns:
                df[col] = None

        df = df[ordered_cols]
        df.to_csv(refined_file_path, index=False)

        return str(refined_file_path)

    extracted_payload = task_1_extrai_dados_api()
    raw_local_file = task_2_salva_raw_local(extracted_payload)

    upload_raw_gcs = LocalFilesystemToGCSOperator(
        task_id="task_3_upload_raw_gcs",
        src=raw_local_file,
        dst=f"{GCS_RAW}/{{{{ ds_nodash }}}}/coincap_assets_raw_{{{{ ts_nodash }}}}.json",
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
    )

    refined_local_file = task_4_transforma_refined(extracted_payload)

    upload_refined_gcs = LocalFilesystemToGCSOperator(
        task_id="task_5_upload_refined_gcs",
        src=refined_local_file,
        dst=f"{GCS_PROCESSED}/{{{{ ds_nodash }}}}/coincap_assets_refined_{{{{ ts_nodash }}}}.csv",
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_trusted_bq = BigQueryInsertJobOperator(
        task_id="task_6_load_trusted_bigquery",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{GCS_BUCKET}/{GCS_PROCESSED}/{{{{ ds_nodash }}}}/coincap_assets_refined_{{{{ ts_nodash }}}}.csv"
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "fieldDelimiter": ",",
                "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
                "autodetect": False,
                "schema": {
                    "fields": [
                        {"name": "id", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "rank", "type": "INT64", "mode": "NULLABLE"},
                        {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "supply", "type": "FLOAT64", "mode": "NULLABLE"},
                        {"name": "maxsupply", "type": "FLOAT64", "mode": "NULLABLE"},
                        {"name": "marketcapusd", "type": "FLOAT64", "mode": "NULLABLE"},
                        {"name": "volumeusd24hr", "type": "FLOAT64", "mode": "NULLABLE"},
                        {"name": "priceusd", "type": "FLOAT64", "mode": "NULLABLE"},
                        {"name": "changepercent24hr", "type": "FLOAT64", "mode": "NULLABLE"},
                        {"name": "vwap24hr", "type": "FLOAT64", "mode": "NULLABLE"},
                        {"name": "explorer", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
                        {"name": "ingestion_date", "type": "DATE", "mode": "NULLABLE"},
                        {"name": "source", "type": "STRING", "mode": "NULLABLE"},
                    ]
                },
                "timePartitioning": {
                    "type": "DAY",
                    "field": "ingestion_date"
                },
                "clustering": {
                    "fields": ["id", "symbol", "name"]
                },
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_views_bq = BigQueryInsertJobOperator(
        task_id="task_7_create_or_replace_views",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_HISTORY}` AS
                SELECT
                  id AS cod_id,
                  rank AS ranking,
                  symbol AS cod_symbol,
                  name AS nome_crypto,
                  supply AS suprimento,
                  maxsupply AS max_suprimento,
                  marketcapusd AS cap_mercado_usd,
                  volumeusd24hr AS volume_usd_24hrs,
                  priceusd AS preco_usd,
                  changepercent24hr AS percent_mudanca_24hrs,
                  vwap24hr AS maior_alta_24hrs,
                  explorer AS endereco_eletronico,
                  source AS arquivo_ref,
                  ingestion_timestamp AS dt_hr_ingestao,
                  DATE(ingestion_timestamp) AS dt_referencia,
                  CASE
                      WHEN changepercent24hr > 0 THEN 'UP'
                      WHEN changepercent24hr < 0 THEN 'DOWN'
                      ELSE 'STABLE'
                  END AS status_preco,
                  CASE
                      WHEN marketcapusd >= 100000000000 THEN 'MEGA CAP'
                      WHEN marketcapusd >= 10000000000 THEN 'LARGE CAP'
                      WHEN marketcapusd >= 1000000000 THEN 'MID CAP'
                      ELSE 'SMALL CAP'
                  END AS classificacao_market_cap
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    extracted_payload >> raw_local_file >> upload_raw_gcs >> refined_local_file >> upload_refined_gcs >> load_trusted_bq >> create_views_bq


coincap_pipeline()