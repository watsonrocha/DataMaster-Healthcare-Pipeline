# Referencia Tecnica dos Modulos

Esta pagina descreve cada modulo do projeto em detalhe, com explicacao das funcoes, parametros e exemplos.

---

## main.py — Orquestrador do Pipeline

**Caminho**: `DTM/DTM/main.py`

Este e o arquivo principal. Ele coordena todas as etapas do pipeline.

### Classe: `HealthcareDataPipeline`

| Metodo | O que faz |
|--------|-----------|
| `__init__()` | Configura logging, cria SparkSession, inicializa todos os componentes |
| `extract()` | Gera dados simulados e salva em CSV/JSON |
| `extract_api()` | Busca dados de APIs publicas (IBGE, COVID-19) |
| `ingest_batch(data_paths)` | Le CSV e JSON, une os DataFrames, verifica qualidade |
| `ingest_streaming()` | Simula ingestao em tempo real (micro-batch) |
| `transform(df)` | Limpa, enriquece e mascara os dados |
| `aggregate(df_silver)` | Cria visoes agregadas (por diagnostico, estado, pressao) |
| `save_to_postgres(df_gold, api_data)` | Salva dados no PostgreSQL (se disponivel) |
| `demo_security()` | Demonstra controle de acesso RBAC |
| `run()` | Executa todas as etapas na ordem correta |

### Funcoes auxiliares (Windows)

| Funcao | O que faz |
|--------|-----------|
| `_setup_java_home()` | Detecta e configura JAVA_HOME automaticamente no Windows. Prioriza Java 17, rejeita Java 23+. |
| `_is_incompatible_java(path)` | Verifica se um JDK e versao 23+ (incompativel com PySpark). |
| `_setup_hadoop_windows()` | Baixa winutils.exe automaticamente do GitHub para compatibilidade Hadoop no Windows. |
| `_setup_windows()` | Chama as 3 funcoes acima + configura PYSPARK_PYTHON. |

---

## config/settings.py — Configuracoes

**Caminho**: `DTM/DTM/config/settings.py`

Centraliza todas as configuracoes do projeto. Usa variaveis de ambiente com valores padrao.

### Classe: `Config`

| Atributo | Padrao | Descricao |
|----------|--------|-----------|
| `SPARK_MASTER` | `local[1]` | Modo de execucao do Spark |
| `SPARK_APP_NAME` | `HealthcareDataPipeline` | Nome da aplicacao Spark |
| `SPARK_EXECUTOR_MEMORY` | `1g` | Memoria por executor |
| `DATA_SOURCES` | dict | URLs de fontes de dados (API gov, Kafka, JDBC) |
| `DATA_LAKE_ROOT` | `output/data_lake` | Raiz do Data Lake |
| `STORAGE` | dict | Caminhos das camadas bronze, silver, gold, checkpoints |
| `ENCRYPTION_KEY` | `chave-segura-exemplo-2025` | Chave para hash SHA-256 |
| `SENSITIVE_FIELDS` | `{cpf, patient_id, nome, email, telefone}` | Campos que serao mascarados |
| `SIMULATED_RECORDS` | `100` | Quantidade de registros gerados |
| `LOG_LEVEL` | `INFO` | Nivel de logging |
| `LOG_FILE` | `output/pipeline.log` | Caminho do arquivo de log |

---

## src/data_extraction/data_generator.py — Gerador de Dados

**Caminho**: `DTM/DTM/src/data_extraction/data_generator.py`

Gera dados ficticios realistas de pacientes brasileiros usando a biblioteca Faker.

### Funcoes

| Funcao | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `generate_healthcare_records(n)` | `n`: numero de registros (padrao: 500) | `list[dict]` | Gera N registros com dados pessoais e clinicos |
| `save_as_csv(records, path)` | registros e caminho | `str` (caminho) | Salva registros em formato CSV |
| `save_as_json(records, path)` | registros e caminho | `str` (caminho) | Salva registros em formato JSON Lines |
| `generate_and_save(n, output_dir)` | quantidade e diretorio | `dict` com caminhos | Gera dados e salva metade em CSV, metade em JSON |

### Campos gerados por registro

| Campo | Tipo | Exemplo | Descricao |
|-------|------|---------|-----------|
| `patient_id` | string | `PAC-847291` | Identificador unico do paciente |
| `cpf` | string | `123.456.789-01` | CPF ficticio (valido algoritmicamente) |
| `nome` | string | `Maria Silva` | Nome completo (Faker pt_BR) |
| `email` | string | `maria@email.com` | Email ficticio |
| `telefone` | string | `(11) 91234-5678` | Telefone brasileiro |
| `idade` | int | `45` | Idade entre 18 e 95 anos |
| `sexo` | string | `M` ou `F` | Sexo biologico |
| `cidade` | string | `Sao Paulo` | Cidade brasileira |
| `estado` | string | `SP` | UF (sigla) |
| `blood_pressure` | string | `120/80` | Pressao arterial (sistolica/diastolica) |
| `heart_rate` | int | `78` | Frequencia cardiaca (50-130 bpm) |
| `temperatura` | float | `37.2` | Temperatura corporal (35.5-40.5°C) |
| `saturacao_o2` | int | `97` | Saturacao de oxigenio (88-100%) |
| `diagnostico` | string | `Hipertensao` | Diagnostico entre 10 opcoes |
| `medicamento` | string | `Losartana` | Medicamento prescrito (pode ser nulo) |
| `timestamp` | string | `2026-04-15 14:30:00` | Data/hora do atendimento |

---

## src/data_extraction/api_extractor.py — APIs Publicas

**Caminho**: `DTM/DTM/src/data_extraction/api_extractor.py`

Busca dados reais de APIs publicas sem autenticacao.

### Funcoes

| Funcao | API | Retorno | Descricao |
|--------|-----|---------|-----------|
| `fetch_ibge_estados()` | IBGE | `list[dict]` (27 itens) | Estados brasileiros com regiao |
| `fetch_covid_brasil()` | Disease.sh | `list[dict]` (1 item) | Dados epidemiologicos do Brasil |
| `fetch_covid_historico(dias)` | Disease.sh | `list[dict]` (N itens) | Historico diario de COVID-19 |
| `fetch_all_api_data()` | Todas | `dict[str, list]` | Busca todas as APIs com tratamento de erros |
| `save_api_data(data, dir)` | — | `dict[str, str]` | Salva os dados como JSON no diretorio |

---

## src/data_ingestion/batch/extractors.py — Extratores Batch

**Caminho**: `DTM/DTM/src/data_ingestion/batch/extractors.py`

Le dados de diversas fontes usando PySpark.

### Classe: `BatchDataExtractor`

| Metodo | Fonte | Descricao |
|--------|-------|-----------|
| `from_csv(path)` | Arquivo CSV | Le CSV com header e inferencia de tipos |
| `from_json(path)` | Arquivo JSON | Le JSON Lines com PySpark |
| `from_parquet(path)` | Arquivo Parquet | Le Parquet diretamente |
| `from_file(path, type)` | Qualquer | Roteador: chama o metodo correto baseado no tipo |
| `from_api(url, params)` | API REST | Faz GET, salva resposta em temp e le com PySpark |
| `from_database(query, url)` | PostgreSQL/JDBC | Le dados de banco de dados via JDBC |

---

## src/data_ingestion/streaming/stream_processor.py — Streaming

**Caminho**: `DTM/DTM/src/data_ingestion/streaming/stream_processor.py`

Processa dados em tempo real (simulado ou Kafka).

### Classe: `StreamProcessor`

| Metodo | Descricao |
|--------|-----------|
| `from_kafka(topic, brokers)` | Conecta a um topico Kafka real e retorna um streaming DataFrame |
| `simulate_stream(n_events)` | Gera N eventos ficticios que simulam dados chegando em tempo real |

### Schema do streaming

```python
SCHEMA = StructType([
    StructField("patient_id", StringType()),
    StructField("cpf", StringType()),
    StructField("nome", StringType()),
    StructField("blood_pressure", StringType()),
    StructField("heart_rate", IntegerType()),
    StructField("temperatura", FloatType()),
    StructField("saturacao_o2", IntegerType()),
    StructField("timestamp", StringType()),
])
```

---

## src/data_processing/batch_transformations.py — Transformacoes

**Caminho**: `DTM/DTM/src/data_processing/batch_transformations.py`

Aplica limpeza, enriquecimento e mascaramento aos dados.

### Classe: `DataTransformer`

| Metodo | Descricao |
|--------|-----------|
| `clean(df)` | Remove duplicatas e registros sem patient_id/timestamp. Adiciona coluna `processing_time`. |
| `enrich_healthcare(df)` | Extrai sistolica/diastolica, categoriza pressao e freq. cardiaca, detecta febre. |
| `apply_masking(df)` | Aplica mascaramento LGPD em todos os campos sensiveis configurados. |
| `transform(df, mask=True)` | Executa o pipeline completo: clean → enrich → mask. |

### Colunas criadas pelo enriquecimento

| Coluna | Tipo | Logica |
|--------|------|--------|
| `sistolica` | int | Extraida de blood_pressure (ex: "120/80" → 120) |
| `diastolica` | int | Extraida de blood_pressure (ex: "120/80" → 80) |
| `categoria_pressao` | string | `baixa` (<90/<60), `normal`, `alta` (>=140/>=90) |
| `categoria_fc` | string | `bradicardia` (<60), `normal`, `taquicardia` (>100) |
| `febre` | boolean | `true` se temperatura >= 37.8°C |
| `processing_time` | timestamp | Horario do processamento |

---

## src/data_storage/data_lake.py — Data Lake Manager

**Caminho**: `DTM/DTM/src/data_storage/data_lake.py`

Gerencia a escrita e leitura de dados no Data Lake em 3 camadas.

### Classe: `DataLakeManager`

| Metodo | Descricao |
|--------|-----------|
| `save_bronze(df, path, partitions)` | Salva DataFrame na camada Bronze (Parquet) |
| `save_silver(df, path, partitions)` | Salva na camada Silver |
| `save_gold(df, path, partitions)` | Salva na camada Gold |
| `read_layer(layer, path)` | Le dados de qualquer camada |
| `stream_to_lake(stream_df, path)` | Escreve streaming para o Data Lake |

---

## src/data_storage/database.py — PostgreSQL

**Caminho**: `DTM/DTM/src/data_storage/database.py`

Gerencia a conexao e escrita no PostgreSQL.

### Funcoes

| Funcao | Descricao |
|--------|-----------|
| `init_database()` | Cria todas as tabelas no PostgreSQL (se nao existirem) |
| `save_dataframe_to_postgres(df, table)` | Salva um DataFrame PySpark em uma tabela PostgreSQL |
| `save_api_data_to_postgres(api_data)` | Salva dados das APIs no PostgreSQL |

### Tabelas criadas

| Tabela | Colunas principais | Origem |
|--------|--------------------|--------|
| `gold_diagnostico` | diagnostico, total_pacientes, medias de sinais vitais | Agregacao Gold |
| `gold_estado` | estado, total_pacientes, pressao_alta | Agregacao Gold |
| `api_estados_ibge` | estado_id, sigla, nome_estado, regiao | API IBGE |
| `api_covid_brasil` | casos_total, mortes_total, recuperados, populacao | API Disease.sh |
| `api_covid_historico` | data, casos_acumulados, mortes_acumuladas | API Disease.sh |

---

## src/monitoring/metrics.py — Observabilidade

**Caminho**: `DTM/DTM/src/monitoring/metrics.py`

Monitora a execucao do pipeline com metricas e alertas.

### Classe: `PipelineMetrics`

| Metodo | Descricao |
|--------|-----------|
| `add_metrics(df)` | Adiciona colunas `processing_lag_ms` e `record_size_bytes` ao DataFrame |
| `check_data_quality(df, stage)` | Analisa: total de registros, colunas com nulos, duplicatas |
| `timer(stage_name)` | Context manager que mede o tempo de execucao de uma etapa |
| `get_timing_report()` | Retorna dicionario com tempos de todas as etapas |
| `alert(message, severity)` | Emite alerta no log |
| `check_thresholds(df)` | Verifica se alguma coluna tem mais de 30% de nulos e dispara alerta |

---

## Proximas paginas

- [Seguranca e LGPD](05-Seguranca-e-LGPD.md)
- [APIs Publicas](06-APIs-Publicas.md)
