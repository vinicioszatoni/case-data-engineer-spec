import logging

from config import COINCAP_ASSET_IDS
from extract import extrai_dados_api
from transform import transformar_json_para_parquet
from upload_gcs import upload_raw, upload_processed
from load_bigquery import carregar_parquet_no_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    if not COINCAP_ASSET_IDS:
        raise ValueError("Nenhum asset_id configurado em COINCAP_ASSET_IDS no .env")

    for asset_id in COINCAP_ASSET_IDS:
        logging.info(f"Iniciando pipeline para: {asset_id}")

        data, local_json_path = extrai_dados_api(asset_id)
        if not data or not local_json_path:
            logging.error(f"Falha na extração para {asset_id}")
            continue

        raw_gcs_uri = upload_raw(local_json_path, asset_id)
        logging.info(f"Raw enviada para: {raw_gcs_uri}")

        _, local_parquet_path = transformar_json_para_parquet(local_json_path, asset_id)
        processed_gcs_uri = upload_processed(local_parquet_path, asset_id)
        logging.info(f"Processed enviado para: {processed_gcs_uri}")

        table_id = carregar_parquet_no_bigquery(processed_gcs_uri)
        logging.info(f"Tabela atualizada: {table_id}")


if __name__ == "__main__":
    main()