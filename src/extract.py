import requests
import json
import logging
from pathlib import Path
from datetime import datetime

from config import (
    CHAVE_API,
    COINCAP_BASE_URL,
    COINCAP_SEARCH,
    COINCAP_LIMIT,
    COINCAP_OFFSET,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def montar_url(asset_id: str) -> str:
    url = (
        f"{COINCAP_BASE_URL}"
        f"?search={COINCAP_SEARCH}"
        f"&ids={asset_id}"
        f"&limit={COINCAP_LIMIT}"
        f"&offset={COINCAP_OFFSET}"
    )
    return url


def extrai_dados_api(asset_id: str) -> tuple[dict, str]:
    if not asset_id:
        raise ValueError("asset_id não informado")

    if not CHAVE_API:
        raise ValueError("CHAVE_API não carregada do arquivo .env")

    if not COINCAP_BASE_URL:
        raise ValueError("COINCAP_BASE_URL não carregada do arquivo .env")

    url = montar_url(asset_id)

    headers = {
        "Authorization": f"Bearer {CHAVE_API}"
    }

    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()

    if response.status_code != 200:
        logging.error(f"Erro na requisição para {asset_id}: {response.status_code} - {data}")
        return {}, ""

    if not data:
        logging.warning(f"Nenhum dado retornado para {asset_id}")
        return {}, ""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(f"data/raw/{asset_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"coincap_{asset_id}_{timestamp}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    logging.info(f"Arquivo salvo localmente em {output_path}")

    return data, str(output_path)