import json
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

NUMERIC_COLUMNS = [
    "supply",
    "maxSupply",
    "marketCapUsd",
    "volumeUsd24Hr",
    "priceUsd",
    "changePercent24Hr",
    "vwap24Hr",
]

INT_COLUMNS = [
    "rank",
]


def snake_case(column_name: str) -> str:

    column_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", column_name)
    column_name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", column_name)
    column_name = re.sub(r"[^a-zA-Z0-9]+", "_", column_name)
    return column_name.lower().strip("_")


def transformar_json_para_parquet(local_json_path: str, asset_id: str) -> tuple[pd.DataFrame, str]:
    json_path = Path(local_json_path)

    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    registros = payload.get("data", [])
    if not registros:
        raise ValueError(f"Nenhum registro encontrado em {local_json_path}")

    df = pd.DataFrame(registros)

    if df.empty:
        raise ValueError(f"DataFrame vazio para {local_json_path}")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Remove coluna problemática para Parquet
    if "tokens" in df.columns:
        df = df.drop(columns=["tokens"])

    # Renomeia colunas para snake_case
    df.columns = [snake_case(col) for col in df.columns]

    # Colunas técnicas
    df["asset_id_param"] = asset_id
    df["source_file"] = json_path.name
    df["ingestion_timestamp"] = pd.Timestamp.utcnow('America/Sao_Paulo')

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(f"data/processed/{asset_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"coincap_{asset_id}_{timestamp}.parquet"
    df.to_parquet(output_path, index=False)

    logging.info(f"Parquet salvo localmente em {output_path}")
    logging.info(f"Colunas finais: {list(df.columns)}")

    return df, str(output_path)