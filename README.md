# Pipeline de Dados de SaÃºde â€” PySpark

> ** Projeto de Engenharia de Dados- Data Master **

Pipeline ETL escalÃ¡vel e seguro para processamento de dados de saÃºde, desenvolvido com **PySpark**. O projeto demonstra as melhores prÃ¡ticas de engenharia de dados, incluindo ingestÃ£o batch/streaming, arquitetura Data Lake (Medallion), mascaramento de dados sensÃ­veis (LGPD), controle de acesso (RBAC) e observabilidade.

---

## Arquitetura de SoluÃ§Ã£o

```
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚                        PIPELINE DE DADOS DE SAÃšDE                       â”‚
â”œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¤
â”‚                                                                          â”‚
â”‚  â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”   â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”   â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”  â”‚
â”‚  â”‚  EXTRAÃ‡ÃƒO    â”‚   â”‚    INGESTÃƒO     â”‚   â”‚     ARMAZENAMENTO         â”‚  â”‚
â”‚  â”‚             â”‚   â”‚                 â”‚   â”‚     (Data Lake)            â”‚  â”‚
â”‚  â”‚ â€¢ CSV       â”‚â”€â”€â–¶â”‚ â€¢ Batch (ETL)   â”‚â”€â”€â–¶â”‚ â€¢ Bronze (dados brutos)   â”‚  â”‚
â”‚  â”‚ â€¢ JSON      â”‚   â”‚ â€¢ Streaming     â”‚   â”‚ â€¢ Silver (dados limpos)   â”‚  â”‚
â”‚  â”‚ â€¢ API IBGE  â”‚   â”‚   (simulado)    â”‚   â”‚ â€¢ Gold   (agregados)      â”‚  â”‚
â”‚  â”‚ â€¢ API COVID â”‚   â”‚                 â”‚   â”‚                           â”‚  â”‚
â”‚  â”‚ â€¢ Simulados â”‚   â”‚ Spark Session   â”‚   â”‚ Formato: Parquet          â”‚  â”‚
â”‚  â”‚ â€¢ Banco     â”‚   â”‚ (local/cluster) â”‚   â”‚ Particionamento por data  â”‚  â”‚
â”‚  â”‚   (JDBC)    â”‚   â”‚                 â”‚   â”‚ + PostgreSQL (opcional)   â”‚  â”‚
â”‚  â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜   â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜   â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜  â”‚
â”‚                                                                          â”‚
â”‚  â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”  â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”   â”‚
â”‚  â”‚   TRANSFORMAÃ‡ÃƒO      â”‚  â”‚        SEGURANÃ‡A & LGPD               â”‚   â”‚
â”‚  â”‚                      â”‚  â”‚                                        â”‚   â”‚
â”‚  â”‚ â€¢ Limpeza de dados   â”‚  â”‚ â€¢ Mascaramento de CPF, nome, email    â”‚   â”‚
â”‚  â”‚ â€¢ RemoÃ§Ã£o duplicatas â”‚  â”‚ â€¢ Hash SHA-256 para patient_id        â”‚   â”‚
â”‚  â”‚ â€¢ CategorizaÃ§Ã£o PA   â”‚  â”‚ â€¢ RBAC (admin, analista, visitante)   â”‚   â”‚
â”‚  â”‚ â€¢ CategorizaÃ§Ã£o FC   â”‚  â”‚ â€¢ Log de auditoria                    â”‚   â”‚
â”‚  â”‚ â€¢ DetecÃ§Ã£o de febre  â”‚  â”‚ â€¢ Criptografia com chave              â”‚   â”‚
â”‚  â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜  â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜   â”‚
â”‚                                                                          â”‚
â”‚  â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”    â”‚
â”‚  â”‚                    OBSERVABILIDADE                              â”‚    â”‚
â”‚  â”‚                                                                 â”‚    â”‚
â”‚  â”‚ â€¢ Logging estruturado (arquivo + console)                      â”‚    â”‚
â”‚  â”‚ â€¢ MÃ©tricas: latÃªncia, tamanho de registros                     â”‚    â”‚
â”‚  â”‚ â€¢ VerificaÃ§Ã£o de qualidade de dados (nulos, duplicatas)        â”‚    â”‚
â”‚  â”‚ â€¢ Alertas automÃ¡ticos para anomalias                           â”‚    â”‚
â”‚  â”‚ â€¢ Timer de cada etapa do pipeline                              â”‚    â”‚
â”‚  â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜    â”‚
â”‚                                                                          â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
```

---

## Requisitos Atendidos

| # | Requisito | ImplementaÃ§Ã£o |
|---|-----------|---------------|
| 1 | **ExtraÃ§Ã£o de Dados** | Dados simulados (Faker) + APIs pÃºblicas reais (IBGE, COVID-19/Disease.sh), CSV/JSON, JDBC |
| 2 | **IngestÃ£o de Dados** | Pipeline batch (ETL) + simulaÃ§Ã£o de streaming; suporte a Kafka (quando disponÃ­vel) |
| 3 | **Armazenamento** | Data Lake Medallion (Bronze/Silver/Gold) em Parquet + **PostgreSQL** para dados agregados |
| 4 | **Observabilidade** | Logging estruturado, mÃ©tricas de latÃªncia, qualidade de dados, alertas automÃ¡ticos |
| 5 | **SeguranÃ§a de Dados** | Criptografia SHA-256, RBAC, log de auditoria, conformidade LGPD |
| 6 | **Mascaramento** | CPF (parcial), nome (pseudonimizaÃ§Ã£o), email (domÃ­nio), telefone, hash de IDs |
| 7 | **Arquitetura** | Data Lake Medallion, processamento distribuÃ­do com Spark, formato columnar (Parquet) |
| 8 | **Escalabilidade** | Spark local[*] â†’ cluster; particionamento por data; processamento paralelo |

---

## Estrutura do Projeto

```
DataMaster-Healthcare-Pipeline/
â”œâ”€â”€ docker-compose.yml                        # PostgreSQL via Docker (dev)
â”œâ”€â”€ docker-compose.prod.yml                   # Stack completa (Pipeline+Kafka+Prometheus+Grafana+Airflow)
â”œâ”€â”€ requirements.txt                          # DependÃªncias (dev)
â”œâ”€â”€ requirements.prod.txt                     # DependÃªncias (produÃ§Ã£o + Delta + Prometheus + Kafka)
â”œâ”€â”€ .github/workflows/
â”‚   â”œâ”€â”€ ci.yml                                # CI: lint, testes, Terraform validate, Docker build
â”‚   â””â”€â”€ cd.yml                                # CD: build ECR, deploy dev/prod
â”œâ”€â”€ infrastructure/
â”‚   â”œâ”€â”€ terraform/                            # IaC AWS (VPC, S3, EMR, RDS, MSK)
â”‚   â”‚   â”œâ”€â”€ main.tf
â”‚   â”‚   â”œâ”€â”€ variables.tf
â”‚   â”‚   â”œâ”€â”€ outputs.tf
â”‚   â”‚   â”œâ”€â”€ environments/{dev,prod}.tfvars
â”‚   â”‚   â””â”€â”€ modules/{vpc,s3,emr,rds,msk}/
â”‚   â””â”€â”€ docker/
â”‚       â”œâ”€â”€ Dockerfile                        # Imagem do pipeline
â”‚       â””â”€â”€ Dockerfile.airflow                # Imagem do Airflow
â”œâ”€â”€ airflow/
â”‚   â””â”€â”€ dags/
â”‚       â”œâ”€â”€ healthcare_pipeline_dag.py        # DAG ETL diÃ¡rio
â”‚       â””â”€â”€ healthcare_streaming_dag.py       # DAG Structured Streaming
â”œâ”€â”€ monitoring/
â”‚   â”œâ”€â”€ prometheus/
â”‚   â”‚   â”œâ”€â”€ prometheus.yml                    # Config Prometheus
â”‚   â”‚   â””â”€â”€ alerts.yml                        # Regras de alerta
â”‚   â””â”€â”€ grafana/
â”‚       â”œâ”€â”€ dashboards/pipeline-overview.json # Dashboard prÃ©-configurado
â”‚       â””â”€â”€ provisioning/                     # Datasources + provisioning
â”œâ”€â”€ docs/
â”‚   â”œâ”€â”€ lgpd/LGPD_TRADEOFFS.md               # Trade-offs LGPD detalhados
â”‚   â””â”€â”€ wiki/                                 # DocumentaÃ§Ã£o completa
â”œâ”€â”€ tests/
â”‚   â”œâ”€â”€ test_security.py                      # Testes RBAC
â”‚   â”œâ”€â”€ test_data_generator.py                # Testes gerador de dados
â”‚   â”œâ”€â”€ test_api_extractor.py                 # Testes APIs
â”‚   â””â”€â”€ test_pyspark_integration.py           # Testes PySpark (transformaÃ§Ãµes, mascaramento, Delta)
â””â”€â”€ DTM/DTM/
    â”œâ”€â”€ main.py                               # Ponto de entrada do pipeline
    â”œâ”€â”€ config/settings.py                    # ConfiguraÃ§Ãµes centralizadas
    â””â”€â”€ src/
        â”œâ”€â”€ data_extraction/
        â”‚   â”œâ”€â”€ data_generator.py             # Gerador de dados simulados
        â”‚   â””â”€â”€ api_extractor.py              # ExtraÃ§Ã£o de APIs pÃºblicas
        â”œâ”€â”€ data_ingestion/
        â”‚   â”œâ”€â”€ batch/extractors.py           # Extratores (CSV, JSON, API, JDBC)
        â”‚   â””â”€â”€ streaming/
        â”‚       â”œâ”€â”€ stream_processor.py       # Streaming simulado
        â”‚       â”œâ”€â”€ kafka_streaming.py        # Structured Streaming real com Kafka
        â”‚       â””â”€â”€ kafka_producer.py         # Producer Kafka para eventos de saÃºde
        â”œâ”€â”€ data_processing/
        â”‚   â””â”€â”€ batch_transformations.py      # Limpeza, enriquecimento, mascaramento
        â”œâ”€â”€ data_storage/
        â”‚   â”œâ”€â”€ data_lake.py                  # Data Lake Manager (Parquet)
        â”‚   â”œâ”€â”€ delta_lake.py                 # Delta Lake Manager (merge, time travel, schema evolution)
        â”‚   â””â”€â”€ database.py                   # PostgreSQL Manager
        â”œâ”€â”€ security/
        â”‚   â”œâ”€â”€ access_control.py             # RBAC + auditoria
        â”‚   â””â”€â”€ data_masking.py               # Mascaramento LGPD
        â””â”€â”€ monitoring/
            â”œâ”€â”€ metrics.py                    # MÃ©tricas base, qualidade, alertas
            â”œâ”€â”€ prometheus_metrics.py         # Exportador Prometheus (counters, histograms, gauges)
            â””â”€â”€ quality_metrics.py            # DQS, estimativa de custo, performance profiling
```

---

## Como Executar

### PrÃ©-requisitos

- **Python** 3.10+
- **Java** 11+ (necessÃ¡rio para PySpark)
- **Docker** (opcional, para PostgreSQL)

### InstalaÃ§Ã£o

```bash
# 1. Clone o repositÃ³rio
git clone https://github.com/watsonrocha/DataMaster-Healthcare-Pipeline.git
cd DataMaster-Healthcare-Pipeline

# 2. Crie um ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Instale as dependÃªncias
pip install -r requirements.txt

# 4. Execute o pipeline
cd DTM/DTM
python main.py
```

### Com PostgreSQL (opcional)

```bash
# Inicie o banco de dados (requer Docker)
docker-compose up -d

# Execute o pipeline (detecta o PostgreSQL automaticamente)
cd DTM/DTM
python main.py

# Verifique os dados no banco
docker exec healthcare_db psql -U pipeline -d healthcare -c "SELECT * FROM gold_diagnostico;"
docker exec healthcare_db psql -U pipeline -d healthcare -c "SELECT * FROM api_covid_brasil;"
```

> **Nota:** O pipeline funciona normalmente **sem PostgreSQL**. Se o banco nÃ£o estiver disponÃ­vel, os dados sÃ£o salvos apenas no Data Lake (Parquet).

### SaÃ­da Esperada

O pipeline gera:
- **Data Lake** em `output/data_lake/` com as camadas Bronze, Silver e Gold
- **Dados de APIs** em `output/data_lake/bronze/api_*.json` (IBGE + COVID-19)
- **PostgreSQL** (se disponÃ­vel): tabelas `gold_diagnostico`, `api_estados_ibge`, `api_covid_brasil`, `api_covid_historico`
- **Log de execuÃ§Ã£o** em `output/pipeline.log`
- **RelatÃ³rio de qualidade** e **mÃ©tricas de desempenho** no console

---

## Detalhamento TÃ©cnico

### 1. ExtraÃ§Ã£o de Dados

**Dados simulados** (`data_generator.py`) â€” gera dados realistas com Faker (pt_BR):
- Dados pessoais: nome, CPF, email, telefone, cidade, estado
- Dados clÃ­nicos: pressÃ£o arterial, frequÃªncia cardÃ­aca, temperatura, saturaÃ§Ã£o Oâ‚‚
- DiagnÃ³sticos: hipertensÃ£o, diabetes, asma, COVID-19, etc.

**APIs pÃºblicas reais** (`api_extractor.py`) â€” sem autenticaÃ§Ã£o:
- **IBGE**: 27 estados brasileiros com regiÃ£o e cÃ³digo
- **Disease.sh (COVID-19)**: dados epidemiolÃ³gicos do Brasil (casos, mortes, recuperados)
- **HistÃ³rico COVID-19**: Ãºltimos 30 dias de dados diÃ¡rios

TambÃ©m suporta extraÃ§Ã£o de **bancos de dados via JDBC**.

### 2. IngestÃ£o de Dados

**Batch (ETL):**
- Leitura de CSV e JSON via PySpark
- UniÃ£o de mÃºltiplos DataFrames (`unionByName`)
- Suporte a Parquet e JDBC

**Streaming (simulado):**
- `StreamProcessor.simulate_stream()` gera micro-batches de dados
- Quando Kafka disponÃ­vel: `StreamProcessor.from_kafka()` para streaming real
- Arquitetura Lambda/Kappa suportada pela combinaÃ§Ã£o batch + stream

### 3. Armazenamento â€” Data Lake (Medallion Architecture)

| Camada | DescriÃ§Ã£o | Formato |
|--------|-----------|---------|
| **Bronze** | Dados brutos, sem transformaÃ§Ã£o | Parquet particionado por data |
| **Silver** | Dados limpos, enriquecidos e mascarados | Parquet particionado por data |
| **Gold** | AgregaÃ§Ãµes prontas para BI/anÃ¡lise | Parquet |

Justificativa do Parquet:
- **Columnar**: otimizado para consultas analÃ­ticas
- **CompressÃ£o**: Snappy por padrÃ£o (~70% menor que CSV)
- **Schema evolution**: suporte a evoluÃ§Ã£o de esquema
- **Particionamento**: consultas por data sem scan completo

### 4. Observabilidade

- **Logging estruturado** com formato `timestamp | level | module | message`
- **Timer** por etapa do pipeline (extraÃ§Ã£o, ingestÃ£o, transformaÃ§Ã£o, etc.)
- **VerificaÃ§Ã£o de qualidade**: contagem de nulos, duplicatas, estatÃ­sticas
- **Alertas automÃ¡ticos**: colunas com >30% de nulos disparam warnings
- **MÃ©tricas em colunas**: `processing_lag_ms` e `record_size_bytes`

### 5. SeguranÃ§a de Dados (LGPD)

- **Criptografia**: SHA-256 com chave para anonimizaÃ§Ã£o de IDs
- **Controle de acesso (RBAC)**: admin, analista, cientista de dados, visitante
- **Log de auditoria**: registra cada acesso com timestamp, usuÃ¡rio, aÃ§Ã£o e autorizaÃ§Ã£o
- **Conformidade LGPD**: dados pessoais sÃ£o mascarados antes do armazenamento na camada Silver

### 6. Mascaramento de Dados

| Campo | TÃ©cnica | Exemplo |
|-------|---------|---------|
| CPF | ExibiÃ§Ã£o parcial | `123.456.789-00` â†’ `***456***` |
| patient_id | Hash SHA-256 + chave | `PAC-123456` â†’ `a3f2c1...` |
| nome | PseudonimizaÃ§Ã£o | `Maria Silva` â†’ `M***_a3f2c1` |
| email | DomÃ­nio preservado | `maria@email.com` â†’ `m***@email.com` |
| telefone | Ãšltimos 4 dÃ­gitos | `(11) 91234-5678` â†’ `(**) *****-5678` |

### 7. Arquitetura de Dados

A arquitetura segue o padrÃ£o **Medallion (Bronze/Silver/Gold)**, amplamente adotado em Data Lakes modernos:

```
Fontes â†’ [Bronze] â†’ [Silver] â†’ [Gold] â†’ BI/AnÃ¡lise
         (bruto)    (limpo)    (agregado)
```

**Tecnologias:**
| Componente | Tecnologia |
|------------|------------|
| Processamento | PySpark (Spark SQL, DataFrames) |
| Streaming | Spark Structured Streaming (simulado) |
| Armazenamento | Data Lake (Parquet columnar) |
| OrquestraÃ§Ã£o | Spark Session (local/cluster) |
| SeguranÃ§a | SHA-256, RBAC, Mascaramento |
| Monitoramento | Logging + MÃ©tricas customizadas |

### 8. Escalabilidade

**Escalabilidade Horizontal:**
- Alterar `SPARK_MASTER` de `local[*]` para `spark://cluster:7077` ou `yarn`
- PySpark distribui automaticamente o processamento entre os workers
- Particionamento por data permite consultas paralelas

**Escalabilidade Vertical:**
- Ajustar `SPARK_EXECUTOR_MEMORY` conforme disponibilidade
- Configurar `spark.sql.shuffle.partitions` para volumes maiores

**EstratÃ©gias para produÃ§Ã£o:**
- OrquestraÃ§Ã£o com **Apache Airflow** para agendamento de jobs
- **Kafka** para streaming real com checkpointing
- **AWS S3** ou **Azure ADLS** como storage do Data Lake
- **Kubernetes** para auto-scaling de workers Spark
- **Delta Lake** para transaÃ§Ãµes ACID no Data Lake

---

## Documentacao Completa (Wiki)

Para uma documentacao detalhada e acessivel (incluindo para leigos), consulte a **[Wiki do projeto](docs/wiki/Home.md)**:

1. [Visao Geral](docs/wiki/01-Visao-Geral.md) â€” O que e o projeto, para que serve
2. [Instalacao e Execucao](docs/wiki/02-Instalacao-e-Execucao.md) â€” Passo a passo para instalar e rodar
3. [Arquitetura](docs/wiki/03-Arquitetura.md) â€” Medallion Architecture, fluxo de dados
4. [Referencia Tecnica](docs/wiki/04-Referencia-Tecnica.md) â€” Descricao de cada modulo, classe e funcao
5. [Seguranca e LGPD](docs/wiki/05-Seguranca-e-LGPD.md) â€” Mascaramento, RBAC, auditoria
6. [APIs Publicas](docs/wiki/06-APIs-Publicas.md) â€” IBGE e Disease.sh
7. [Banco de Dados](docs/wiki/07-Banco-de-Dados.md) â€” PostgreSQL, Docker, consultas SQL
8. [Glossario](docs/wiki/08-Glossario.md) â€” Termos tecnicos explicados
9. [Deploy na Nuvem AWS e Observabilidade](docs/wiki/09-Deploy-AWS-e-Observabilidade.md) â€” Deploy real na AWS (S3 + RDS via Terraform), modelo hibrido, decisao de custo, Grafana lendo o RDS

---

## Infraestrutura Cloud (AWS)

O projeto foi **efetivamente deployado na AWS** num modelo **hibrido**: o armazenamento (S3 + RDS) fica na nuvem e o processamento roda local (Docker). Toda a infraestrutura e provisionada via **Terraform (IaC)**.

Existem duas versoes da infraestrutura:

| Versao | Recursos | Custo aprox. | Quando usar |
|--------|----------|--------------|-------------|
| **Enxuta** (`infrastructure/terraform-lean/`) â€” **a que esta no ar** | S3 (Data Lake) + RDS PostgreSQL + security group | **~US$ 0** (Free Tier) | Provar o projeto na nuvem, portfolio, estudo |
| **Completa** (`infrastructure/terraform/`) | VPC + NAT + S3 + EMR (Spark) + MSK (Kafka) + RDS | ~US$ 400/mes | Producao real, alto volume |

### Versao enxuta (deploy real, baixo custo)

```
infrastructure/terraform-lean/
â”œâ”€â”€ main.tf                     # S3 (Data Lake) + RDS PostgreSQL + security group
â”œâ”€â”€ variables.tf                # VariÃ¡veis configurÃ¡veis (regiÃ£o, senha, etc.)
â”œâ”€â”€ outputs.tf                  # Outputs (endpoint do RDS, nomes dos buckets)
â””â”€â”€ lean.tfvars                 # Config (us-east-1, db.t3.micro)
```

```bash
# Deploy (versao enxuta â€” o que foi realmente provisionado)
cd infrastructure/terraform-lean
export TF_VAR_db_password="<senha-do-banco>"   # nunca commitada
terraform init
terraform plan  -var-file=lean.tfvars
terraform apply -var-file=lean.tfvars

# Para zerar o custo quando terminar:
terraform destroy -var-file=lean.tfvars
```

**O que fica rodando na AWS:** S3 Data Lake (Bronze/Silver/Gold em Parquet), bucket de checkpoints e RDS PostgreSQL 16 (`db.t3.micro`) com as tabelas Gold (`gold_diagnostico`, `gold_estado`, `api_*`).

### Versao completa (producao)

```
infrastructure/terraform/
â”œâ”€â”€ main.tf                     # OrquestraÃ§Ã£o dos mÃ³dulos
â”œâ”€â”€ variables.tf                # VariÃ¡veis configurÃ¡veis
â”œâ”€â”€ outputs.tf                  # Outputs (endpoints, IDs)
â”œâ”€â”€ environments/
â”‚   â”œâ”€â”€ dev.tfvars              # Config para desenvolvimento
â”‚   â””â”€â”€ prod.tfvars             # Config para produÃ§Ã£o
â””â”€â”€ modules/
    â”œâ”€â”€ vpc/                    # VPC, subnets, NAT Gateway
    â”œâ”€â”€ s3/                     # Data Lake S3 com lifecycle
    â”œâ”€â”€ emr/                    # Cluster Spark (EMR)
    â”œâ”€â”€ rds/                    # PostgreSQL (RDS)
    â””â”€â”€ msk/                    # Kafka (MSK)
```

```bash
# Deploy (versao completa)
cd infrastructure/terraform
terraform init
terraform plan -var-file="environments/dev.tfvars"
terraform apply -var-file="environments/dev.tfvars"
```

> Detalhes completos do deploy, modelo hibrido e decisao de custo: **[Wiki â€” Deploy na Nuvem AWS e Observabilidade](docs/wiki/09-Deploy-AWS-e-Observabilidade.md)**.

---

## Streaming Real com Kafka

Substitui o streaming simulado por **Structured Streaming** real com Apache Kafka:

- **Checkpoint** para exactly-once semantics
- **Watermark** para tratamento de dados atrasados (10 min)
- **Schema enforcement** com validaÃ§Ã£o
- **AgregaÃ§Ãµes com janela temporal** deslizante (5 min window, 1 min slide)
- **Producer** com `acks=all` e retry
- **Consumer** com `maxOffsetsPerTrigger` para controle de backpressure

```bash
# Iniciar ambiente completo com Kafka
docker compose -f docker-compose.prod.yml up -d
```

---

## OrquestraÃ§Ã£o com Airflow

Duas DAGs prontas para produÃ§Ã£o:

| DAG | Schedule | DescriÃ§Ã£o |
|-----|----------|-----------|
| `healthcare_etl_pipeline` | DiÃ¡rio 02:00 UTC | Pipeline ETL completo (Bronze â†’ Silver â†’ Gold) |
| `healthcare_streaming_pipeline` | @once | Gerencia Structured Streaming com Kafka |

Funcionalidades: retry automÃ¡tico, branch para PostgreSQL, quality check, alertas por email.

---

## CI/CD com GitHub Actions

| Workflow | Trigger | Etapas |
|----------|---------|--------|
| **CI** | Push/PR | Lint (Ruff), Unit tests, PySpark integration tests, Terraform validate, Docker build |
| **CD** | Push main/tags | Build & push ECR, Deploy dev (main), Deploy prod (tags v*) |

---

## Observabilidade com Prometheus + Grafana

Dois tipos de dashboard, que respondem perguntas diferentes:

**a) Operacional (Prometheus â†’ Grafana)** â€” "o pipeline esta saudavel?"
- **Prometheus**: scrape de mÃ©tricas do pipeline, Spark, Kafka, PostgreSQL
- **Grafana**: dashboard prÃ©-configurado com 7 painÃ©is
- **Alertas**: pipeline errors, stage timeout, high null ratio, streaming lag, no processing

MÃ©tricas expostas em `:8000/metrics`:
- `pipeline_records_processed_total`
- `pipeline_stage_duration_seconds`
- `pipeline_data_quality_null_ratio`
- `pipeline_errors_total`
- `pipeline_streaming_lag_seconds`

**b) NegÃ³cio (Grafana â†’ RDS na AWS)** â€” "o que os dados dizem?"
- Datasource PostgreSQL apontando direto para o **RDS na nuvem** (provisionado via YAML)
- Dashboard **"Healthcare Gold â€” RDS (AWS)"** lÃª as tabelas Gold: 220 pacientes, 11 diagnÃ³sticos, sinais vitais por diagnÃ³stico
- Senha do RDS via `.env` (no `.gitignore`), nunca no cÃ³digo

Detalhes: **[Wiki â€” Deploy na Nuvem AWS e Observabilidade](docs/wiki/09-Deploy-AWS-e-Observabilidade.md)**.

---

## Delta Lake

SubstituiÃ§Ã£o do Parquet simples por **Delta Lake** com:

| Funcionalidade | DescriÃ§Ã£o |
|----------------|-----------|
| **MERGE (Upsert)** | Evita duplicatas com condiÃ§Ã£o de join configurÃ¡vel |
| **SCD Type 2** | MantÃ©m histÃ³rico de alteraÃ§Ãµes (is_current, valid_from, valid_to) |
| **Schema Evolution** | `mergeSchema=true` para evoluÃ§Ã£o automÃ¡tica |
| **Time Travel** | Leitura por versÃ£o (`versionAsOf`) ou timestamp (`timestampAsOf`) |
| **VACUUM** | Limpeza de arquivos antigos com retenÃ§Ã£o configurÃ¡vel |
| **Z-ORDER** | OtimizaÃ§Ã£o de layout para consultas frequentes |
| **Streaming Sink** | Escrita de streams com checkpoint e merge schema |

---

## MÃ©tricas Reais de Qualidade, Custo e Performance

### Data Quality Score (DQS)

Score composicional com 4 dimensÃµes ponderadas:

| DimensÃ£o | Peso | O que mede |
|----------|------|------------|
| Completude | 30% | ProporÃ§Ã£o de valores nÃ£o-nulos |
| Unicidade | 25% | ProporÃ§Ã£o de registros Ãºnicos |
| ConsistÃªncia | 25% | Valores dentro de faixas vÃ¡lidas (FC, temp, SpO2, idade) |
| Atualidade | 20% | Idade dos dados vs. threshold |

### Estimativa de Custos AWS

| Componente | PrecificaÃ§Ã£o |
|------------|-------------|
| EMR (m5.xlarge) | $0.126/hora/nÃ³ |
| S3 Storage | $0.023/GB/mÃªs |
| Data Transfer | $0.09/GB |
| RDS (t3.medium) | $0.068/hora |
| MSK (m5.large) | $0.21/hora/broker |

### Performance Profiling

- DuraÃ§Ã£o por estÃ¡gio com identificaÃ§Ã£o de bottleneck
- Records/segundo por estÃ¡gio
- % do tempo total por estÃ¡gio
- MÃ©tricas do Spark (jobs, stages, paralelismo)

---

## ExecuÃ§Ã£o ReproduzÃ­vel com Docker

```bash
# Ambiente completo (Pipeline + Kafka + PostgreSQL + Prometheus + Grafana + Airflow)
docker compose -f docker-compose.prod.yml up -d

# Acessos:
#   Pipeline metrics: http://localhost:8000/metrics
#   Spark UI:         http://localhost:4040
#   PostgreSQL:       localhost:5432
#   Kafka:            localhost:9092
#   Prometheus:       http://localhost:9090
#   Grafana:          http://localhost:3000 (admin/admin)
#   Airflow:          http://localhost:8080
```

---

## DocumentaÃ§Ã£o LGPD

DocumentaÃ§Ã£o detalhada de trade-offs em [`docs/lgpd/LGPD_TRADEOFFS.md`](docs/lgpd/LGPD_TRADEOFFS.md):

- Trade-offs de cada tÃ©cnica de mascaramento
- Alternativas consideradas e recomendaÃ§Ãµes
- Design do RBAC e limitaÃ§Ãµes
- Requisitos de auditoria (Arts. 37, 38, 46, 48, 49)
- Direitos do titular (Arts. 17-22)
- ClassificaÃ§Ã£o de dados por camada
- Matriz de riscos LGPD
- Custos de conformidade

---

## Tecnologias

| Categoria | Tecnologias |
|-----------|-------------|
| Processamento | PySpark 3.5+, Delta Lake 3.1+ |
| Streaming | Apache Kafka, Structured Streaming |
| OrquestraÃ§Ã£o | Apache Airflow 2.8+ |
| Armazenamento | S3, Delta Lake, PostgreSQL 16 |
| Infraestrutura | Terraform, Docker, AWS (EMR, RDS, MSK, S3) |
| Observabilidade | Prometheus, Grafana, CloudWatch |
| CI/CD | GitHub Actions, Docker, ECR |
| SeguranÃ§a | SHA-256, RBAC, LGPD compliance |
| Testes | pytest, PySpark integration tests |
| Linguagem | Python 3.10+ |

---

## Autor

**Watson Rocha** 2026

