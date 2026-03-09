CREATE OR REPLACE VIEW `projeto-etl-espec1.case_spec_data_engineer.vw_coincap_assets` AS
SELECT
    id AS cod_id,
    rank AS ranking,
    symbol AS cod_symbol,
    name AS nome_crypto,
    supply AS suprimento,
    max_supply AS max_suprimento,
    market_cap_usd AS,
    volume_usd24_hr AS volume_usd_24hrs,
    price_usd AS preco_usd,
    change_percent24_hr AS percent_mudanca_24hrs,
    vwap24_hr AS maior_alta_24hrs
    explorer AS endereco_eletronico,
    asset_id_param AS cod_id_crypto,
    source_file AS arquivo_ref,
    ingestion_timestamp AS dt_hr_ingestao,
    DATE(ingestion_timestamp) AS dt_referencia,
    CASE
        WHEN change_percent24_hr > 0 THEN 'UP'
        WHEN change_percent24_hr < 0 THEN 'DOWN'
        ELSE 'STABLE'
    END AS status_preco
    CASE
        WHEN market_cap_usd >= 100000000000 THEN 'MEGA CAP'
        WHEN market_cap_usd >= 10000000000 THEN 'LARGE CAP'
        WHEN market_cap_usd >= 1000000000 THEN 'MID CAP'
        ELSE 'SMALL CAP'
    END AS classificacao_market_cap
FROM `projeto-etl-espec1.case_spec_data_engineer.coincap_assets`
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY ingestion_timestamp DESC
) = 1;