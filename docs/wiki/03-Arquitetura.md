# Arquitetura do Projeto

## Visao geral da arquitetura

O projeto segue o padrao **Medallion Architecture** (Bronze/Silver/Gold), amplamente usado em Data Lakes modernos. Cada camada representa um nivel de refinamento dos dados.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                    FONTES DE DADOS (Extracao)                               │
│                                                                             │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│   │ Faker    │  │ API IBGE │  │ API      │  │ Kafka    │                  │
│   │ (simul.) │  │ (estados)│  │ COVID-19 │  │ (futuro) │                  │
│   └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘                  │
│        │              │              │              │                        │
│        └──────────────┴──────────────┴──────────────┘                       │
│                              │                                              │
│                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │                    CAMADA BRONZE                                │       │
│   │                    (Dados Brutos)                               │       │
│   │                                                                 │       │
│   │   Dados no formato original, sem nenhuma transformacao.         │       │
│   │   Servem como backup e fonte de verdade.                        │       │
│   │                                                                 │       │
│   │   Formatos: CSV, JSON Lines, Parquet, JSON                      │       │
│   │   Conteudo: pacientes_batch1.csv, pacientes_batch2.jsonl,       │       │
│   │             api_estados_ibge.json, api_covid_brasil.json,       │       │
│   │             api_covid_historico.json, healthcare/ (Parquet)      │       │
│   └──────────────────────────────┬──────────────────────────────────┘       │
│                                  │                                          │
│                                  ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │                    CAMADA SILVER                                │       │
│   │                    (Dados Limpos)                               │       │
│   │                                                                 │       │
│   │   Dados limpos, enriquecidos e com campos sensiveis mascarados. │       │
│   │   Prontos para analise exploratoria.                            │       │
│   │                                                                 │       │
│   │   Transformacoes aplicadas:                                     │       │
│   │   - Remocao de duplicatas                                       │       │
│   │   - Split da pressao arterial (sistolica/diastolica)            │       │
│   │   - Categorizacao (pressao: alta/normal/baixa)                  │       │
│   │   - Categorizacao (freq. cardiaca: bradicardia/normal/taqui.)   │       │
│   │   - Deteccao de febre (temperatura >= 37.8°C)                   │       │
│   │   - Mascaramento LGPD (CPF, nome, email, telefone, patient_id) │       │
│   │   - Metricas de processamento (lag, tamanho)                    │       │
│   │                                                                 │       │
│   │   Formato: Parquet particionado por data                        │       │
│   └──────────────────────────────┬──────────────────────────────────┘       │
│                                  │                                          │
│                                  ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │                    CAMADA GOLD                                  │       │
│   │                    (Dados Agregados)                            │       │
│   │                                                                 │       │
│   │   Dados agregados e prontos para consumo por ferramentas de BI. │       │
│   │                                                                 │       │
│   │   Visoes disponíveis:                                           │       │
│   │   - por_diagnostico: media de sinais vitais por diagnostico     │       │
│   │   - por_estado: total de pacientes e pressao alta por estado    │       │
│   │   - por_pressao: distribuicao por categoria de pressao          │       │
│   │                                                                 │       │
│   │   Formato: Parquet + PostgreSQL (opcional)                      │       │
│   └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Por que o padrao Medallion?

| Vantagem | Explicacao |
|----------|------------|
| **Rastreabilidade** | Dados brutos sempre preservados na camada Bronze (nunca sao alterados) |
| **Qualidade incremental** | Cada camada refina os dados: bruto → limpo → agregado |
| **Reprocessamento** | Se uma transformacao estiver errada, basta reprocessar a partir do Bronze |
| **Separacao de responsabilidades** | Analistas acessam Silver, executivos acessam Gold |

---

## Por que Parquet?

O projeto usa o formato **Apache Parquet** para armazenar os dados no Data Lake. Comparado com CSV:

| Caracteristica | CSV | Parquet |
|----------------|-----|---------|
| Tipo de armazenamento | Linha por linha | Coluna por coluna |
| Tamanho em disco | Grande | ~70% menor (compressao Snappy) |
| Velocidade de leitura | Lenta (le tudo) | Rapida (le so colunas necessarias) |
| Schema embutido | Nao | Sim (tipos definidos no arquivo) |
| Suporte a particoes | Nao | Sim (particao por data, estado, etc.) |

---

## Estrutura de pastas do codigo

```
DataMasterFinal/
│
├── docker-compose.yml          # Define o PostgreSQL via Docker
├── requirements.txt            # Dependencias Python do projeto
├── README.md                   # Documentacao principal
├── .gitignore                  # Arquivos ignorados pelo Git
│
├── docs/
│   └── wiki/                   # Documentacao Wiki completa
│       ├── 01-Visao-Geral.md
│       ├── 02-Instalacao-e-Execucao.md
│       ├── 03-Arquitetura.md
│       ├── 04-Referencia-Tecnica.md
│       ├── 05-Seguranca-e-LGPD.md
│       ├── 06-APIs-Publicas.md
│       ├── 07-Banco-de-Dados.md
│       └── 08-Glossario.md
│
└── DTM/DTM/                    # Codigo-fonte principal
    │
    ├── main.py                 # Ponto de entrada — orquestra todo o pipeline
    │
    ├── config/
    │   ├── __init__.py
    │   └── settings.py         # Configuracoes centralizadas (Spark, paths, LGPD)
    │
    ├── src/
    │   ├── data_extraction/    # ETAPA 1: Extracao de dados
    │   │   ├── __init__.py
    │   │   ├── data_generator.py   # Gera dados ficticios com Faker
    │   │   └── api_extractor.py    # Busca dados de APIs publicas
    │   │
    │   ├── data_ingestion/     # ETAPA 2: Ingestao de dados
    │   │   ├── batch/
    │   │   │   ├── __init__.py
    │   │   │   └── extractors.py   # Le CSV, JSON, Parquet, API, JDBC
    │   │   └── streaming/
    │   │       ├── __init__.py
    │   │       └── stream_processor.py  # Simula ingestao em tempo real
    │   │
    │   ├── data_processing/    # ETAPA 3: Transformacao
    │   │   ├── __init__.py
    │   │   └── batch_transformations.py  # Limpeza, enriquecimento, mascaramento
    │   │
    │   ├── data_storage/       # ETAPA 4: Armazenamento
    │   │   ├── __init__.py
    │   │   ├── data_lake.py    # Gerencia as camadas Bronze/Silver/Gold
    │   │   └── database.py     # Salva dados no PostgreSQL
    │   │
    │   ├── security/           # Seguranca e conformidade
    │   │   ├── __init__.py
    │   │   ├── access_control.py   # RBAC: controle de acesso por perfil
    │   │   └── data_masking.py     # Mascaramento LGPD de campos sensiveis
    │   │
    │   └── monitoring/         # Observabilidade
    │       ├── __init__.py
    │       └── metrics.py      # Metricas, qualidade de dados, alertas
    │
    └── output/                 # Gerado em tempo de execucao
        ├── data_lake/
        │   ├── bronze/
        │   ├── silver/
        │   └── gold/
        └── pipeline.log
```

---

## Fluxo de execucao detalhado

Quando voce roda `python main.py`, as seguintes etapas acontecem na ordem:

### Etapa 0: Configuracao automatica
- Detecta o sistema operacional
- No Windows: configura JAVA_HOME automaticamente (prioriza Java 17)
- No Windows: baixa o winutils.exe (necessario para Hadoop)
- Cria a SparkSession com configuracoes otimizadas

### Etapa 1A: Extracao de dados simulados
- Gera 100 registros ficticios de pacientes (configuravel)
- Salva metade em CSV e metade em JSON Lines na camada Bronze

### Etapa 1B: Extracao de APIs publicas
- Busca 27 estados brasileiros da API do IBGE
- Busca dados epidemiologicos COVID-19 da API Disease.sh
- Busca historico de 30 dias de COVID-19
- Salva tudo na camada Bronze como arquivos JSON

### Etapa 2: Ingestao Batch
- Le o arquivo CSV com PySpark
- Le o arquivo JSON com PySpark
- Une os dois DataFrames em um so
- Verifica a qualidade dos dados (nulos, duplicatas)
- Salva na camada Bronze em formato Parquet

### Etapa 3: Ingestao Streaming (simulada)
- Gera 20 eventos de "tempo real" simulados
- Combina com os dados batch
- Se falhar (compatibilidade), o pipeline continua apenas com batch

### Etapa 4: Transformacao (Bronze → Silver)
- Limpa os dados (remove duplicatas, registros invalidos)
- Enriquece: separa pressao arterial em sistolica/diastolica
- Categoriza: pressao (alta/normal/baixa), freq. cardiaca, febre
- Mascara campos sensiveis conforme LGPD
- Salva na camada Silver em Parquet

### Etapa 5: Agregacao (Silver → Gold)
- Agrupa dados por diagnostico (media de sinais vitais)
- Agrupa por estado (total de pacientes, pressao alta)
- Agrupa por categoria de pressao
- Salva na camada Gold em Parquet

### Etapa 6: Salvamento no PostgreSQL
- Se PostgreSQL estiver disponivel, salva os dados Gold e APIs nas tabelas
- Se nao estiver disponivel, apenas informa e continua

### Etapa 7: Demonstracao de seguranca
- Mostra os 4 perfis de usuario e suas permissoes
- Demonstra acesso negado para usuario sem permissao

### Etapa 8: Relatorio final
- Exibe tempos de execucao de cada etapa
- Mostra amostra dos dados agregados (Gold)

---

## Proximas paginas

- [Referencia tecnica dos modulos](04-Referencia-Tecnica.md)
- [Seguranca e LGPD](05-Seguranca-e-LGPD.md)
