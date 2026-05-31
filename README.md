# Pipeline de Dados de Saúde — PySpark

> ** Projeto de Engenharia de Dados- Data Master **

Pipeline ETL escalável e seguro para processamento de dados de saúde, desenvolvido com **PySpark**. O projeto demonstra as melhores práticas de engenharia de dados, incluindo ingestão batch/streaming, arquitetura Data Lake (Medallion), mascaramento de dados sensíveis (LGPD), controle de acesso (RBAC) e observabilidade.

---

## Arquitetura de Solução

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        PIPELINE DE DADOS DE SAÚDE                       │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────┐   ┌─────────────────┐   ┌───────────────────────────┐  │
│  │  EXTRAÇÃO    │   │    INGESTÃO     │   │     ARMAZENAMENTO         │  │
│  │             │   │                 │   │     (Data Lake)            │  │
│  │ • CSV       │──▶│ • Batch (ETL)   │──▶│ • Bronze (dados brutos)   │  │
│  │ • JSON      │   │ • Streaming     │   │ • Silver (dados limpos)   │  │
│  │ • API IBGE  │   │   (simulado)    │   │ • Gold   (agregados)      │  │
│  │ • API COVID │   │                 │   │                           │  │
│  │ • Simulados │   │ Spark Session   │   │ Formato: Parquet          │  │
│  │ • Banco     │   │ (local/cluster) │   │ Particionamento por data  │  │
│  │   (JDBC)    │   │                 │   │ + PostgreSQL (opcional)   │  │
│  └─────────────┘   └─────────────────┘   └───────────────────────────┘  │
│                                                                          │
│  ┌──────────────────────┐  ┌────────────────────────────────────────┐   │
│  │   TRANSFORMAÇÃO      │  │        SEGURANÇA & LGPD               │   │
│  │                      │  │                                        │   │
│  │ • Limpeza de dados   │  │ • Mascaramento de CPF, nome, email    │   │
│  │ • Remoção duplicatas │  │ • Hash SHA-256 para patient_id        │   │
│  │ • Categorização PA   │  │ • RBAC (admin, analista, visitante)   │   │
│  │ • Categorização FC   │  │ • Log de auditoria                    │   │
│  │ • Detecção de febre  │  │ • Criptografia com chave              │   │
│  └──────────────────────┘  └────────────────────────────────────────┘   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    OBSERVABILIDADE                              │    │
│  │                                                                 │    │
│  │ • Logging estruturado (arquivo + console)                      │    │
│  │ • Métricas: latência, tamanho de registros                     │    │
│  │ • Verificação de qualidade de dados (nulos, duplicatas)        │    │
│  │ • Alertas automáticos para anomalias                           │    │
│  │ • Timer de cada etapa do pipeline                              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Requisitos Atendidos

| # | Requisito | Implementação |
|---|-----------|---------------|
| 1 | **Extração de Dados** | Dados simulados (Faker) + APIs públicas reais (IBGE, COVID-19/Disease.sh), CSV/JSON, JDBC |
| 2 | **Ingestão de Dados** | Pipeline batch (ETL) + simulação de streaming; suporte a Kafka (quando disponível) |
| 3 | **Armazenamento** | Data Lake Medallion (Bronze/Silver/Gold) em Parquet + **PostgreSQL** para dados agregados |
| 4 | **Observabilidade** | Logging estruturado, métricas de latência, qualidade de dados, alertas automáticos |
| 5 | **Segurança de Dados** | Criptografia SHA-256, RBAC, log de auditoria, conformidade LGPD |
| 6 | **Mascaramento** | CPF (parcial), nome (pseudonimização), email (domínio), telefone, hash de IDs |
| 7 | **Arquitetura** | Data Lake Medallion, processamento distribuído com Spark, formato columnar (Parquet) |
| 8 | **Escalabilidade** | Spark local[*] → cluster; particionamento por data; processamento paralelo |

---

## Estrutura do Projeto

```
DataMaster-Healthcare-Pipeline/
├── docker-compose.yml                        # PostgreSQL via Docker (dev)
├── docker-compose.prod.yml                   # Stack completa (Pipeline+Kafka+Prometheus+Grafana+Airflow)
├── requirements.txt                          # Dependências (dev)
├── requirements.prod.txt                     # Dependências (produção + Delta + Prometheus + Kafka)
├── .github/workflows/
│   ├── ci.yml                                # CI: lint, testes, Terraform validate, Docker build
│   └── cd.yml                                # CD: build ECR, deploy dev/prod
├── infrastructure/
│   ├── terraform/                            # IaC AWS (VPC, S3, EMR, RDS, MSK)
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── environments/{dev,prod}.tfvars
│   │   └── modules/{vpc,s3,emr,rds,msk}/
│   └── docker/
│       ├── Dockerfile                        # Imagem do pipeline
│       └── Dockerfile.airflow                # Imagem do Airflow
├── airflow/
│   └── dags/
│       ├── healthcare_pipeline_dag.py        # DAG ETL diário
│       └── healthcare_streaming_dag.py       # DAG Structured Streaming
├── monitoring/
│   ├── prometheus/
│   │   ├── prometheus.yml                    # Config Prometheus
│   │   └── alerts.yml                        # Regras de alerta
│   └── grafana/
│       ├── dashboards/pipeline-overview.json # Dashboard pré-configurado
│       └── provisioning/                     # Datasources + provisioning
├── docs/
│   ├── lgpd/LGPD_TRADEOFFS.md               # Trade-offs LGPD detalhados
│   └── wiki/                                 # Documentação completa
├── tests/
│   ├── test_security.py                      # Testes RBAC
│   ├── test_data_generator.py                # Testes gerador de dados
│   ├── test_api_extractor.py                 # Testes APIs
│   └── test_pyspark_integration.py           # Testes PySpark (transformações, mascaramento, Delta)
└── DTM/DTM/
    ├── main.py                               # Ponto de entrada do pipeline
    ├── config/settings.py                    # Configurações centralizadas
    └── src/
        ├── data_extraction/
        │   ├── data_generator.py             # Gerador de dados simulados
        │   └── api_extractor.py              # Extração de APIs públicas
        ├── data_ingestion/
        │   ├── batch/extractors.py           # Extratores (CSV, JSON, API, JDBC)
        │   └── streaming/
        │       ├── stream_processor.py       # Streaming simulado
        │       ├── kafka_streaming.py        # Structured Streaming real com Kafka
        │       └── kafka_producer.py         # Producer Kafka para eventos de saúde
        ├── data_processing/
        │   └── batch_transformations.py      # Limpeza, enriquecimento, mascaramento
        ├── data_storage/
        │   ├── data_lake.py                  # Data Lake Manager (Parquet)
        │   ├── delta_lake.py                 # Delta Lake Manager (merge, time travel, schema evolution)
        │   └── database.py                   # PostgreSQL Manager
        ├── security/
        │   ├── access_control.py             # RBAC + auditoria
        │   └── data_masking.py               # Mascaramento LGPD
        └── monitoring/
            ├── metrics.py                    # Métricas base, qualidade, alertas
            ├── prometheus_metrics.py         # Exportador Prometheus (counters, histograms, gauges)
            └── quality_metrics.py            # DQS, estimativa de custo, performance profiling
```

---

## Como Executar

### Pré-requisitos

- **Python** 3.10+
- **Java** 11+ (necessário para PySpark)
- **Docker** (opcional, para PostgreSQL)

### Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/watsonrocha/DataMaster-Healthcare-Pipeline.git
cd DataMaster-Healthcare-Pipeline

# 2. Crie um ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Instale as dependências
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

> **Nota:** O pipeline funciona normalmente **sem PostgreSQL**. Se o banco não estiver disponível, os dados são salvos apenas no Data Lake (Parquet).

### Saída Esperada

O pipeline gera:
- **Data Lake** em `output/data_lake/` com as camadas Bronze, Silver e Gold
- **Dados de APIs** em `output/data_lake/bronze/api_*.json` (IBGE + COVID-19)
- **PostgreSQL** (se disponível): tabelas `gold_diagnostico`, `api_estados_ibge`, `api_covid_brasil`, `api_covid_historico`
- **Log de execução** em `output/pipeline.log`
- **Relatório de qualidade** e **métricas de desempenho** no console

---

## Detalhamento Técnico

### 1. Extração de Dados

**Dados simulados** (`data_generator.py`) — gera dados realistas com Faker (pt_BR):
- Dados pessoais: nome, CPF, email, telefone, cidade, estado
- Dados clínicos: pressão arterial, frequência cardíaca, temperatura, saturação O₂
- Diagnósticos: hipertensão, diabetes, asma, COVID-19, etc.

**APIs públicas reais** (`api_extractor.py`) — sem autenticação:
- **IBGE**: 27 estados brasileiros com região e código
- **Disease.sh (COVID-19)**: dados epidemiológicos do Brasil (casos, mortes, recuperados)
- **Histórico COVID-19**: últimos 30 dias de dados diários

Também suporta extração de **bancos de dados via JDBC**.

### 2. Ingestão de Dados

**Batch (ETL):**
- Leitura de CSV e JSON via PySpark
- União de múltiplos DataFrames (`unionByName`)
- Suporte a Parquet e JDBC

**Streaming (simulado):**
- `StreamProcessor.simulate_stream()` gera micro-batches de dados
- Quando Kafka disponível: `StreamProcessor.from_kafka()` para streaming real
- Arquitetura Lambda/Kappa suportada pela combinação batch + stream

### 3. Armazenamento — Data Lake (Medallion Architecture)

| Camada | Descrição | Formato |
|--------|-----------|---------|
| **Bronze** | Dados brutos, sem transformação | Parquet particionado por data |
| **Silver** | Dados limpos, enriquecidos e mascarados | Parquet particionado por data |
| **Gold** | Agregações prontas para BI/análise | Parquet |

Justificativa do Parquet:
- **Columnar**: otimizado para consultas analíticas
- **Compressão**: Snappy por padrão (~70% menor que CSV)
- **Schema evolution**: suporte a evolução de esquema
- **Particionamento**: consultas por data sem scan completo

### 4. Observabilidade

- **Logging estruturado** com formato `timestamp | level | module | message`
- **Timer** por etapa do pipeline (extração, ingestão, transformação, etc.)
- **Verificação de qualidade**: contagem de nulos, duplicatas, estatísticas
- **Alertas automáticos**: colunas com >30% de nulos disparam warnings
- **Métricas em colunas**: `processing_lag_ms` e `record_size_bytes`

### 5. Segurança de Dados (LGPD)

- **Criptografia**: SHA-256 com chave para anonimização de IDs
- **Controle de acesso (RBAC)**: admin, analista, cientista de dados, visitante
- **Log de auditoria**: registra cada acesso com timestamp, usuário, ação e autorização
- **Conformidade LGPD**: dados pessoais são mascarados antes do armazenamento na camada Silver

### 6. Mascaramento de Dados

| Campo | Técnica | Exemplo |
|-------|---------|---------|
| CPF | Exibição parcial | `123.456.789-00` → `***456***` |
| patient_id | Hash SHA-256 + chave | `PAC-123456` → `a3f2c1...` |
| nome | Pseudonimização | `Maria Silva` → `M***_a3f2c1` |
| email | Domínio preservado | `maria@email.com` → `m***@email.com` |
| telefone | Últimos 4 dígitos | `(11) 91234-5678` → `(**) *****-5678` |

### 7. Arquitetura de Dados

A arquitetura segue o padrão **Medallion (Bronze/Silver/Gold)**, amplamente adotado em Data Lakes modernos:

```
Fontes → [Bronze] → [Silver] → [Gold] → BI/Análise
         (bruto)    (limpo)    (agregado)
```

**Tecnologias:**
| Componente | Tecnologia |
|------------|------------|
| Processamento | PySpark (Spark SQL, DataFrames) |
| Streaming | Spark Structured Streaming (simulado) |
| Armazenamento | Data Lake (Parquet columnar) |
| Orquestração | Spark Session (local/cluster) |
| Segurança | SHA-256, RBAC, Mascaramento |
| Monitoramento | Logging + Métricas customizadas |

### 8. Escalabilidade

**Escalabilidade Horizontal:**
- Alterar `SPARK_MASTER` de `local[*]` para `spark://cluster:7077` ou `yarn`
- PySpark distribui automaticamente o processamento entre os workers
- Particionamento por data permite consultas paralelas

**Escalabilidade Vertical:**
- Ajustar `SPARK_EXECUTOR_MEMORY` conforme disponibilidade
- Configurar `spark.sql.shuffle.partitions` para volumes maiores

**Estratégias para produção:**
- Orquestração com **Apache Airflow** para agendamento de jobs
- **Kafka** para streaming real com checkpointing
- **AWS S3** ou **Azure ADLS** como storage do Data Lake
- **Kubernetes** para auto-scaling de workers Spark
- **Delta Lake** para transações ACID no Data Lake

---

## Documentacao Completa (Wiki)

Para uma documentacao detalhada e acessivel (incluindo para leigos), consulte a **[Wiki do projeto](docs/wiki/Home.md)**:

1. [Visao Geral](docs/wiki/01-Visao-Geral.md) — O que e o projeto, para que serve
2. [Instalacao e Execucao](docs/wiki/02-Instalacao-e-Execucao.md) — Passo a passo para instalar e rodar
3. [Arquitetura](docs/wiki/03-Arquitetura.md) — Medallion Architecture, fluxo de dados
4. [Referencia Tecnica](docs/wiki/04-Referencia-Tecnica.md) — Descricao de cada modulo, classe e funcao
5. [Seguranca e LGPD](docs/wiki/05-Seguranca-e-LGPD.md) — Mascaramento, RBAC, auditoria
6. [APIs Publicas](docs/wiki/06-APIs-Publicas.md) — IBGE e Disease.sh
7. [Banco de Dados](docs/wiki/07-Banco-de-Dados.md) — PostgreSQL, Docker, consultas SQL
8. [Glossario](docs/wiki/08-Glossario.md) — Termos tecnicos explicados
9. [Deploy na Nuvem AWS e Observabilidade](docs/wiki/09-Deploy-AWS-e-Observabilidade.md) — Deploy real na AWS (S3 + RDS via Terraform), modelo hibrido, decisao de custo, Grafana lendo o RDS

---

## Infraestrutura Cloud (AWS)

O projeto foi **efetivamente deployado na AWS** num modelo **hibrido**: o armazenamento (S3 + RDS) fica na nuvem e o processamento roda local (Docker). Toda a infraestrutura e provisionada via **Terraform (IaC)**.

Existem duas versoes da infraestrutura:

| Versao | Recursos | Custo aprox. | Quando usar |
|--------|----------|--------------|-------------|
| **Enxuta** (`infrastructure/terraform-lean/`) — **a que esta no ar** | S3 (Data Lake) + RDS PostgreSQL + security group | **~US$ 0** (Free Tier) | Provar o projeto na nuvem, portfolio, estudo |
| **Completa** (`infrastructure/terraform/`) | VPC + NAT + S3 + EMR (Spark) + MSK (Kafka) + RDS | ~US$ 400/mes | Producao real, alto volume |

### Versao enxuta (deploy real, baixo custo)

```
infrastructure/terraform-lean/
├── main.tf                     # S3 (Data Lake) + RDS PostgreSQL + security group
├── variables.tf                # Variáveis configuráveis (região, senha, etc.)
├── outputs.tf                  # Outputs (endpoint do RDS, nomes dos buckets)
└── lean.tfvars                 # Config (us-east-1, db.t3.micro)
```

```bash
# Deploy (versao enxuta — o que foi realmente provisionado)
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
├── main.tf                     # Orquestração dos módulos
├── variables.tf                # Variáveis configuráveis
├── outputs.tf                  # Outputs (endpoints, IDs)
├── environments/
│   ├── dev.tfvars              # Config para desenvolvimento
│   └── prod.tfvars             # Config para produção
└── modules/
    ├── vpc/                    # VPC, subnets, NAT Gateway
    ├── s3/                     # Data Lake S3 com lifecycle
    ├── emr/                    # Cluster Spark (EMR)
    ├── rds/                    # PostgreSQL (RDS)
    └── msk/                    # Kafka (MSK)
```

```bash
# Deploy (versao completa)
cd infrastructure/terraform
terraform init
terraform plan -var-file="environments/dev.tfvars"
terraform apply -var-file="environments/dev.tfvars"
```

> Detalhes completos do deploy, modelo hibrido e decisao de custo: **[Wiki — Deploy na Nuvem AWS e Observabilidade](docs/wiki/09-Deploy-AWS-e-Observabilidade.md)**.

---

## Streaming Real com Kafka

Substitui o streaming simulado por **Structured Streaming** real com Apache Kafka:

- **Checkpoint** para exactly-once semantics
- **Watermark** para tratamento de dados atrasados (10 min)
- **Schema enforcement** com validação
- **Agregações com janela temporal** deslizante (5 min window, 1 min slide)
- **Producer** com `acks=all` e retry
- **Consumer** com `maxOffsetsPerTrigger` para controle de backpressure

```bash
# Iniciar ambiente completo com Kafka
docker compose -f docker-compose.prod.yml up -d
```

---

## Orquestração com Airflow

Duas DAGs prontas para produção:

| DAG | Schedule | Descrição |
|-----|----------|-----------|
| `healthcare_etl_pipeline` | Diário 02:00 UTC | Pipeline ETL completo (Bronze → Silver → Gold) |
| `healthcare_streaming_pipeline` | @once | Gerencia Structured Streaming com Kafka |

Funcionalidades: retry automático, branch para PostgreSQL, quality check, alertas por email.

---

## CI/CD com GitHub Actions

| Workflow | Trigger | Etapas |
|----------|---------|--------|
| **CI** | Push/PR | Lint (Ruff), Unit tests, PySpark integration tests, Terraform validate, Docker build |
| **CD** | Push main/tags | Build & push ECR, Deploy dev (main), Deploy prod (tags v*) |

---

## Observabilidade com Prometheus + Grafana

Dois tipos de dashboard, que respondem perguntas diferentes:

**a) Operacional (Prometheus → Grafana)** — "o pipeline esta saudavel?"
- **Prometheus**: scrape de métricas do pipeline, Spark, Kafka, PostgreSQL
- **Grafana**: dashboard pré-configurado com 7 painéis
- **Alertas**: pipeline errors, stage timeout, high null ratio, streaming lag, no processing

Métricas expostas em `:8000/metrics`:
- `pipeline_records_processed_total`
- `pipeline_stage_duration_seconds`
- `pipeline_data_quality_null_ratio`
- `pipeline_errors_total`
- `pipeline_streaming_lag_seconds`

**b) Negócio (Grafana → RDS na AWS)** — "o que os dados dizem?"
- Datasource PostgreSQL apontando direto para o **RDS na nuvem** (provisionado via YAML)
- Dashboard **"Healthcare Gold — RDS (AWS)"** lê as tabelas Gold: 220 pacientes, 11 diagnósticos, sinais vitais por diagnóstico
- Senha do RDS via `.env` (no `.gitignore`), nunca no código

Detalhes: **[Wiki — Deploy na Nuvem AWS e Observabilidade](docs/wiki/09-Deploy-AWS-e-Observabilidade.md)**.

---

## Delta Lake

Substituição do Parquet simples por **Delta Lake** com:

| Funcionalidade | Descrição |
|----------------|-----------|
| **MERGE (Upsert)** | Evita duplicatas com condição de join configurável |
| **SCD Type 2** | Mantém histórico de alterações (is_current, valid_from, valid_to) |
| **Schema Evolution** | `mergeSchema=true` para evolução automática |
| **Time Travel** | Leitura por versão (`versionAsOf`) ou timestamp (`timestampAsOf`) |
| **VACUUM** | Limpeza de arquivos antigos com retenção configurável |
| **Z-ORDER** | Otimização de layout para consultas frequentes |
| **Streaming Sink** | Escrita de streams com checkpoint e merge schema |

---

## Métricas Reais de Qualidade, Custo e Performance

### Data Quality Score (DQS)

Score composicional com 4 dimensões ponderadas:

| Dimensão | Peso | O que mede |
|----------|------|------------|
| Completude | 30% | Proporção de valores não-nulos |
| Unicidade | 25% | Proporção de registros únicos |
| Consistência | 25% | Valores dentro de faixas válidas (FC, temp, SpO2, idade) |
| Atualidade | 20% | Idade dos dados vs. threshold |

### Estimativa de Custos AWS

| Componente | Precificação |
|------------|-------------|
| EMR (m5.xlarge) | $0.126/hora/nó |
| S3 Storage | $0.023/GB/mês |
| Data Transfer | $0.09/GB |
| RDS (t3.medium) | $0.068/hora |
| MSK (m5.large) | $0.21/hora/broker |

### Performance Profiling

- Duração por estágio com identificação de bottleneck
- Records/segundo por estágio
- % do tempo total por estágio
- Métricas do Spark (jobs, stages, paralelismo)

---

## Execução Reproduzível com Docker

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

## Documentação LGPD

Documentação detalhada de trade-offs em [`docs/lgpd/LGPD_TRADEOFFS.md`](docs/lgpd/LGPD_TRADEOFFS.md):

- Trade-offs de cada técnica de mascaramento
- Alternativas consideradas e recomendações
- Design do RBAC e limitações
- Requisitos de auditoria (Arts. 37, 38, 46, 48, 49)
- Direitos do titular (Arts. 17-22)
- Classificação de dados por camada
- Matriz de riscos LGPD
- Custos de conformidade

---

## Tecnologias

| Categoria | Tecnologias |
|-----------|-------------|
| Processamento | PySpark 3.5+, Delta Lake 3.1+ |
| Streaming | Apache Kafka, Structured Streaming |
| Orquestração | Apache Airflow 2.8+ |
| Armazenamento | S3, Delta Lake, PostgreSQL 16 |
| Infraestrutura | Terraform, Docker, AWS (EMR, RDS, MSK, S3) |
| Observabilidade | Prometheus, Grafana, CloudWatch |
| CI/CD | GitHub Actions, Docker, ECR |
| Segurança | SHA-256, RBAC, LGPD compliance |
| Testes | pytest, PySpark integration tests |
| Linguagem | Python 3.10+ |

---

## Autor

**Watson Rocha** 2026
