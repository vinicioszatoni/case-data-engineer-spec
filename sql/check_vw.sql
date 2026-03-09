SELECT
  asset_id_param,
  symbol,
  name,
  rank,
  price_usd,
  market_cap_usd,
  volume_usd24_hr,
  change_percent24_hr,
  ingestion_timestamp
FROM `projeto-etl-espec1.case_spec_data_engineer.vw_coincap_assets`
ORDER BY rank;

SELECT
  COUNT(*) AS total_linhas,
  COUNT(DISTINCT id) AS total_ativos,
  COUNTIF(price_usd IS NULL) AS price_usd_nulo,
  COUNTIF(symbol IS NULL) AS symbol_nulo
FROM `projeto-etl-espec1.case_spec_data_engineer.vw_coincap_assets`;

SELECT
  id,
  COUNT(*) AS qtd
FROM `projeto-etl-espec1.case_spec_data_engineer.vw_coincap_assets`
GROUP BY id
HAVING COUNT(*) > 1;