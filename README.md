# ETL Extracao Dados API CoinCap e Carregamento no GCP | Data Engineering Pipeline

Pipeline de Engenharia de Dados para extração de dados da API CoinCap, armazenamento em camadas no Google Cloud Storage, transformação para Parquet, carga no BigQuery e disponibilização de views analíticas para consumo no Looker Studio.

---

## Visão Geral

Este projeto implementa um pipeline ponta a ponta para ingestão, transformação e disponibilização analítica de dados de criptomoedas consumidos da API CoinCap.

A solução foi construída com foco em boas práticas de Engenharia de Dados, incluindo:

* separação em camadas `raw`, `processed` e `gold`
* armazenamento do dado bruto antes de qualquer transformação
* padronização de schema para consumo analítico
* versionamento de scripts SQL no projeto
* integração com BigQuery e Looker Studio
* escalabilidade para múltiplos ativos configuráveis via arquivo `.env`

---

## Objetivo do Projeto

O objetivo principal deste projeto é demonstrar a construção de um pipeline moderno de dados no ecossistema Google Cloud Platform, aplicando conceitos de:

* ingestão via API REST
* armazenamento em Data Lake
* transformação em formato colunar
* modelagem analítica no BigQuery
* disponibilização para consumo em dashboards

Além disso, o projeto foi estruturado para servir como base evolutiva para futuras implementações com Docker, Airflow e dbt.

---

## Arquitetura da Solução

```text
CoinCap API
   ↓
Python Extract
   ↓
JSON local
   ↓
GCS Raw
   ↓
Transformação em Parquet
   ↓
GCS Processed
   ↓
BigQuery Table
   ↓
Views SQL (Gold / History / Quality)
   ↓
Looker Studio
```

### Fluxo resumido

1. O pipeline consome dados da API CoinCap para uma lista configurável de criptoativos.
2. Os dados são salvos localmente em formato JSON.
3. Os arquivos JSON são enviados para a camada `raw` no Google Cloud Storage.
4. Os dados são transformados em DataFrames Pandas.
5. O resultado transformado é salvo em formato Parquet.
6. Os arquivos Parquet são enviados para a camada `processed` no GCS.
7. Os arquivos processados são carregados em uma tabela histórica no BigQuery.
8. Views SQL são criadas para consumo analítico e visualização no Looker Studio.

---

## Tecnologias Utilizadas

* **Python**
* **Google Cloud Platform (GCP)**

  * Google Cloud Storage (GCS)
  * BigQuery
  * gcloud CLI
* **Pandas**
* **PyArrow**
* **Requests**
* **python-dotenv**
* **SQL (BigQuery Standard SQL)**
* **Looker Studio**
* **VS Code**

---

## Estrutura do Projeto

```text
etl_case_data_engineer/
├── .env
├── .gitignore
├── README.md
├── dags/
|   ├── pipeline_coincap_dag.py
├── logs
├── notebooks
├── plugins
├── source
├── requirements.txt
├── data/
│   ├── raw/
│   │   ├── bitcoin/
│   │   └── ethereum/
│   └── processed/
│       ├── bitcoin/
│       └── ethereum/
├── sql/
│   ├── gold_coincap.sql
│   ├── vw_historico.sql
│   └── check_vw.sql
└── src/
    ├── config.py
    ├── extract.py
    ├── transform.py
    ├── upload_gcs.py
    ├── load_bigquery.py
    └── main.py
```

---

## Fonte de Dados

### API utilizada

* **CoinCap API**
* Endpoint base:

```text
https://rest.coincap.io/v3/assets
```

### Ativos configuráveis

O pipeline foi desenhado para trabalhar com uma lista dinâmica de ativos definida no arquivo `.env`.

Exemplo:

```env
COINCAP_ASSET_IDS=bitcoin,ethereum
```

No futuro, novos ativos podem ser adicionados sem alteração de código, apenas alterando essa configuração.

Exemplo:

```env
COINCAP_ASSET_IDS=bitcoin,ethereum,solana,cardano,dogecoin
```

---

## Configuração do Ambiente

### Pré-requisitos

Antes de executar o projeto, é necessário ter:

* Python 3.10 ou superior
* conta no Google Cloud Platform
* projeto ativo no GCP
* bucket criada no Google Cloud Storage
* dataset criado no BigQuery
* Google Cloud CLI instalada e autenticada
* ambiente virtual Python criado
* bibliotecas do projeto instaladas

### Instalação das dependências

```bash
pip install -r requirements.txt
```

Caso ainda não exista o `requirements.txt`, as bibliotecas principais utilizadas no projeto são:

```bash
pip install requests pandas pyarrow python-dotenv google-cloud-storage google-cloud-bigquery
```

---

## Exemplo de Configuração do `.env`

```env
CHAVE_API=YOUR_API_KEY

COINCAP_BASE_URL=https://rest.coincap.io/v3/assets
COINCAP_ASSET_IDS=bitcoin,ethereum
COINCAP_SEARCH=
COINCAP_LIMIT=100
COINCAP_OFFSET=0

GCP_PROJECT_ID=projeto-etl-espec1
GCS_BUCKET=projeto-espec-data-engineer
GCS_RAW=raw/coincap/assets
GCS_PROCESSED=processed/coincap/assets

BQ_DATASET=case_spec_data_engineer
BQ_TABLE=coincap_assets
```

> O arquivo `.env` contém informações sensíveis e não deve ser versionado no repositório.

---

## Como Executar o Pipeline

### 1. Ativar o ambiente virtual

```bash
source .venv/bin/activate
```

### 2. Executar o pipeline completo

```bash
PYTHONPATH=src python src/main.py
```

Esse fluxo executa automaticamente:

* extração da API
* salvamento local em JSON
* upload da camada `raw` para o GCS
* transformação para Parquet
* upload da camada `processed` para o GCS
* carga no BigQuery

---

## Organização em Camadas

### Raw

Camada responsável por armazenar os dados brutos da API em formato JSON, preservando a resposta original da fonte.

Exemplo no GCS:

```text
gs://projeto-espec-data-engineer/raw/coincap/assets/bitcoin/2026/03/08/coincap_bitcoin_20260308_193000.json
```

### Processed

Camada responsável por armazenar os dados já tratados em formato Parquet, mais adequado para análise e carga no BigQuery.

Exemplo no GCS:

```text
gs://projeto-espec-data-engineer/processed/coincap/assets/bitcoin/2026/03/08/coincap_bitcoin_20260308_193000.parquet
```

### Gold / Analytics

Camada analítica criada no BigQuery com SQL, aplicando regras de negócio e preparando os dados para consumo em dashboards.

---

## Componentes do Projeto

### `src/config.py`

Arquivo responsável por centralizar a leitura das variáveis do arquivo `.env`.

### `src/extract.py`

Responsável por:

* montar a URL dinamicamente por ativo
* consumir a API CoinCap
* salvar os dados em JSON localmente

### `src/transform.py`

Responsável por:

* ler os arquivos JSON da camada raw
* acessar a chave `data`
* transformar os dados em DataFrame
* remover a coluna `tokens`
* converter colunas numéricas
* padronizar os nomes das colunas em `snake_case`
* adicionar colunas técnicas
* gerar arquivos Parquet

### `src/upload_gcs.py`

Responsável por enviar arquivos para o Google Cloud Storage, tanto na camada raw quanto na camada processed.

### `src/load_bigquery.py`

Responsável por carregar os arquivos Parquet armazenados no GCS para o BigQuery.

### `src/main.py`

Arquivo principal responsável por orquestrar a execução local de todas as etapas do pipeline.

---

## Transformações Aplicadas

Durante a etapa de transformação, foram aplicadas as seguintes regras:

* leitura da chave `data` do JSON retornado pela API
* remoção da coluna `tokens` devido à incompatibilidade com `PyArrow/Parquet`
* conversão de colunas numéricas
* conversão da coluna `rank` para inteiro
* renomeação das colunas para padrão `snake_case`
* adição das colunas técnicas:

  * `asset_id_param`
  * `source_file`
  * `ingestion_timestamp`

### Exemplo de colunas finais

* `id`
* `rank`
* `symbol`
* `name`
* `supply`
* `max_supply`
* `market_cap_usd`
* `volume_usd24_hr`
* `price_usd`
* `change_percent24_hr`
* `vwap24_hr`
* `explorer`
* `asset_id_param`
* `source_file`
* `ingestion_timestamp`

---

## BigQuery

### Tabela principal

```text
projeto-etl-espec1.case_spec_data_engineer.coincap_assets
```

Essa tabela armazena o histórico das cargas em modo append.

---

## Views Criadas

### 1. View Gold

```text
projeto-etl-espec1.case_spec_data_engineer.vw_coincap_assets
```

View analítica criada para consumo refinado, com regras de negócio aplicadas.

Campos e enriquecimentos principais:

* `reference_date`
* `price_status`
* `market_cap_band`

### 2. View Histórica

```text
projeto-etl-espec1.case_spec_data_engineer.vw_coincap_assets_history
```

View histórica criada para análises temporais no Looker Studio.

Campos adicionais:

* `reference_date`
* `reference_datetime`
* `reference_hour`
* `price_status`
* `market_cap_band`

Essa view é ideal para:

* séries temporais
* evolução de preço
* evolução de volume
* evolução de market cap

---

## Qualidade de Dados

Foi criado um conjunto inicial de checagens de qualidade por meio do arquivo SQL:

```text
sql/quality_checks.sql
```

As validações incluem:

* contagem total de linhas
* ativos distintos
* checagem de nulos em colunas críticas
* duplicidade por ativo e timestamp
* valores numéricos suspeitos
* validação da view latest
* validação da view histórica

---

## SQL Versionado no Projeto

Os scripts SQL foram mantidos na pasta `sql/`, permitindo:

* versionamento da lógica analítica
* rastreabilidade das alterações
* reuso futuro em orquestração
* organização mais profissional do projeto

Arquivos principais:

* `gold_coincap.sql`
* `vw_historico.sql`
* `check_vw.sql`

---

## Integração com Looker Studio

A view `vw_coincap_assets` foi conectada ao Looker Studio para construção de dashboards analíticos.

Casos de uso possíveis:

* comparação entre criptoativos
* acompanhamento de preço atual
* evolução histórica de preço
* análise de volume transacionado
* acompanhamento de market cap
* filtros por ativo e status de mercado

A view histórica `vw_coincap_assets_history` pode ser usada para construção de gráficos de linha e análises temporais.

---

## Boas Práticas Aplicadas

* separação em camadas `raw`, `processed` e `gold`
* preservação do dado bruto antes da transformação
* variáveis sensíveis isoladas em `.env`
* código modularizado por responsabilidade
* uso de Parquet para maior eficiência analítica
* padronização de colunas em `snake_case`
* versionamento dos scripts SQL
* uso de BigQuery como camada de analytics
* projeto desenhado para evolução futura com múltiplos ativos

---

## Lições Técnicas

Durante a construção do projeto, alguns pontos técnicos importantes foram resolvidos:

### Variáveis do `.env` não carregadas

Foi necessário ajustar o carregamento do `.env` no `config.py` apontando explicitamente para a raiz do projeto.

### Erro no upload para GCS

A variável `GCS_BUCKET` não deve conter o prefixo `gs://`, apenas o nome da bucket.

### Erro de escrita no Parquet

A coluna `tokens` retornada pela API continha um `struct` vazio (`{}`), incompatível com o writer do PyArrow. A solução adotada foi remover essa coluna na etapa de transformação.

### Problema de localização no BigQuery

As execuções SQL no BigQuery precisaram ser realizadas informando a `location` correta do dataset.

---

## Próximos Passos

As próximas evoluções planejadas para o projeto são:

* criação de novas views analíticas
* melhoria das regras de negócio da camada Gold
* ampliação das validações de qualidade de dados
* documentação complementar do schema
* criação de `Dockerfile` para padronização do ambiente
* orquestração do pipeline com Airflow
* adoção de dbt para testes e governança analítica
* evolução para cargas incrementais mais avançadas

---

## Valor Técnico do Projeto

Este projeto demonstra competências práticas em:

* consumo de APIs
* engenharia de dados em Python
* armazenamento em Data Lake
* transformação de dados em formato colunar
* integração com Google Cloud Platform
* modelagem analítica no BigQuery
* versionamento de SQL
* disponibilização para BI

---

## Autor

**Vinicios Cordeiro**
Projeto de Engenharia de Dados com foco em pipeline analítico no Google Cloud Platform.

---

## Licença

Este projeto possui caráter educacional, técnico e demonstrativo, sendo destinado a estudo, prática, portfólio e evolução profissional em Engenharia de Dados.
# case-data-engineer-spec
