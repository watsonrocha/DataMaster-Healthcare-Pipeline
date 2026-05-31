# Instalacao e Execucao

Este guia explica passo a passo como instalar e executar o projeto em **Windows**, **Linux** e **Mac**.

---

## Pre-requisitos

Antes de comecar, voce precisa ter instalado:

### 1. Python (obrigatorio)

- **Versao**: 3.10 ou superior
- **Download**: https://www.python.org/downloads/
- **No Windows**: durante a instalacao, marque a opcao **"Add Python to PATH"**
- **Verificar**: abra o terminal e digite:
  ```bash
  python --version
  # Deve mostrar algo como: Python 3.13.x
  ```

### 2. Java JDK (obrigatorio)

O PySpark precisa do Java para funcionar internamente. **Recomendamos o Java 17** (versao LTS mais estavel).

- **Download**: https://adoptium.net/temurin/releases/?version=17
  - Escolha: **JDK 17 LTS** > seu sistema operacional > x64 > **.msi** (Windows) ou **.tar.gz** (Linux)
- **No Windows**: durante a instalacao, marque **"Set JAVA_HOME variable"**
- **Verificar**:
  ```bash
  java -version
  # Deve mostrar: openjdk version "17.x.x"
  ```

> **IMPORTANTE**: Java 23 ou superior **nao** funciona com o PySpark! Use Java 11, 17 ou 21.

### 3. Git (recomendado)

Para clonar o repositorio e receber atualizacoes facilmente.

- **Download**: https://git-scm.com/downloads
- **Verificar**:
  ```bash
  git --version
  ```

### 4. Docker (opcional — para PostgreSQL)

Necessario apenas se quiser salvar os dados no banco PostgreSQL.

- **Download**: https://www.docker.com/products/docker-desktop/
- **Verificar**:
  ```bash
  docker --version
  docker-compose --version
  ```

---

## Instalacao passo a passo

### Passo 1: Baixar o projeto

**Opcao A — Via Git (recomendado):**
```bash
git clone https://github.com/watsonrocha/DataMasterFinal.git
cd DataMasterFinal
```

**Opcao B — Via ZIP:**
1. Acesse: https://github.com/watsonrocha/DataMasterFinal
2. Clique em **Code** > **Download ZIP**
3. Extraia o arquivo ZIP
4. Abra o terminal na pasta extraida

### Passo 2: Criar ambiente virtual

Um ambiente virtual isola as dependencias do projeto, evitando conflitos com outros projetos Python.

**Linux/Mac:**
```bash
python -m venv venv
source venv/bin/activate
```

**Windows (PowerShell):**
```powershell
python -m venv venv
venv\Scripts\activate
```

> Voce sabera que o ambiente virtual esta ativo quando ver `(venv)` no inicio da linha do terminal.

### Passo 3: Instalar dependencias

```bash
pip install -r requirements.txt
```

Isso instala automaticamente:
- `pyspark` — framework de processamento de dados
- `requests` — para chamadas de API
- `python-dotenv` — para variaveis de ambiente
- `Faker` — para gerar dados ficticios
- `psycopg2-binary` — para conexao com PostgreSQL

### Passo 4: Executar o pipeline

```bash
cd DTM/DTM
python main.py
```

---

## Saida esperada

Ao executar, voce vera algo como:

```
JAVA_HOME configurado automaticamente: C:\Program Files\Eclipse Adoptium\jdk-17...

2026-05-03 17:38:38 | INFO | pipeline | ============================================================
2026-05-03 17:38:38 | INFO | pipeline | INICIANDO PIPELINE DE DADOS DE SAUDE
2026-05-03 17:38:38 | INFO | pipeline | ============================================================

... [varias linhas de processamento] ...

2026-05-03 17:38:59 | INFO | pipeline | ============================================================
2026-05-03 17:38:59 | INFO | pipeline | PIPELINE CONCLUIDO COM SUCESSO
2026-05-03 17:38:59 | INFO | pipeline | ============================================================
2026-05-03 17:38:59 | INFO | pipeline |   Extracao de Dados (simulados)      -> 0.01s
2026-05-03 17:38:59 | INFO | pipeline |   Extracao de Dados (APIs publicas)  -> 2.24s
2026-05-03 17:38:59 | INFO | pipeline |   Ingestao Batch                     -> 7.21s
2026-05-03 17:38:59 | INFO | pipeline |   Ingestao Streaming (simulada)      -> 1.03s
2026-05-03 17:38:59 | INFO | pipeline |   Transformacao e Mascaramento       -> 5.30s
2026-05-03 17:38:59 | INFO | pipeline |   Agregacao (Gold)                   -> 1.43s
2026-05-03 17:38:59 | INFO | pipeline |   Salvamento PostgreSQL              -> 0.00s
2026-05-03 17:38:59 | INFO | pipeline |   TEMPO TOTAL                        -> 14.98s
```

### Arquivos gerados

Apos a execucao, a pasta `output/` contera:

```
output/
├── data_lake/
│   ├── bronze/                        # Dados brutos
│   │   ├── pacientes_batch1.csv       # Metade dos registros em CSV
│   │   ├── pacientes_batch2.jsonl     # Metade em JSON Lines
│   │   ├── api_estados_ibge.json      # 27 estados do Brasil
│   │   ├── api_covid_brasil.json      # Dados COVID-19
│   │   ├── api_covid_historico.json   # Historico 30 dias
│   │   └── healthcare/               # Dados em Parquet
│   ├── silver/                        # Dados limpos e mascarados
│   │   └── healthcare/               # Dados em Parquet
│   └── gold/                          # Dados agregados
│       ├── por_diagnostico/           # Agrupado por diagnostico
│       ├── por_estado/                # Agrupado por estado
│       └── por_pressao/              # Agrupado por categoria de pressao
└── pipeline.log                       # Log completo da execucao
```

---

## Execucao com PostgreSQL (opcional)

Para salvar os dados tambem em um banco PostgreSQL:

### 1. Iniciar o banco

```bash
# Na raiz do projeto (onde esta o docker-compose.yml)
docker-compose up -d
```

Isso cria um container com PostgreSQL 16 ja configurado:
- **Banco**: `healthcare`
- **Usuario**: `pipeline`
- **Senha**: `pipeline123`
- **Porta**: `5432`

### 2. Executar o pipeline

```bash
cd DTM/DTM
python main.py
```

O pipeline detecta automaticamente o PostgreSQL e salva os dados.

### 3. Consultar os dados no banco

```bash
# Ver todas as tabelas
docker exec healthcare_db psql -U pipeline -d healthcare -c "\dt"

# Ver dados agregados por diagnostico
docker exec healthcare_db psql -U pipeline -d healthcare -c "SELECT * FROM gold_diagnostico;"

# Ver estados do IBGE
docker exec healthcare_db psql -U pipeline -d healthcare -c "SELECT * FROM api_estados_ibge;"

# Ver dados COVID-19
docker exec healthcare_db psql -U pipeline -d healthcare -c "SELECT * FROM api_covid_brasil;"
```

### 4. Parar o banco

```bash
docker-compose down        # Para o container (dados persistem)
docker-compose down -v     # Para e APAGA todos os dados
```

---

## Variaveis de ambiente (opcionais)

Voce pode customizar o comportamento do pipeline usando variaveis de ambiente:

| Variavel | Padrao | Descricao |
|----------|--------|-----------|
| `SPARK_MASTER` | `local[1]` | Modo do Spark (`local[1]`, `local[*]`, `spark://host:port`) |
| `SPARK_EXECUTOR_MEMORY` | `1g` | Memoria por executor Spark |
| `SIMULATED_RECORDS` | `100` | Quantidade de registros simulados gerados |
| `LOG_LEVEL` | `INFO` | Nivel de log (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `DB_HOST` | `localhost` | Host do PostgreSQL |
| `DB_PORT` | `5432` | Porta do PostgreSQL |
| `DB_NAME` | `healthcare` | Nome do banco de dados |
| `DB_USER` | `pipeline` | Usuario do banco |
| `DB_PASSWORD` | `pipeline123` | Senha do banco |
| `JAVA_HOME` | (auto-detectado) | Caminho do Java JDK |

**Exemplo de uso (Windows PowerShell):**
```powershell
$env:SIMULATED_RECORDS = "500"
$env:SPARK_MASTER = "local[2]"
python main.py
```

**Exemplo de uso (Linux/Mac):**
```bash
SIMULATED_RECORDS=500 SPARK_MASTER="local[2]" python main.py
```

---

## Solucao de problemas comuns

### "ModuleNotFoundError: No module named 'pyspark'"
```bash
pip install -r requirements.txt
```

### "JAVA_HOME is not set"
Instale o Java 17: https://adoptium.net/temurin/releases/?version=17

No Windows, se o Java esta instalado mas nao e detectado:
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
python main.py
```

### "getSubject is not supported"
Voce esta usando Java 23 ou superior. Instale o **Java 17** (LTS). O pipeline prioriza Java 17 automaticamente, mas se o Java 23+ for o unico instalado, ele nao funcionara.

### "Python worker exited unexpectedly (crashed)"
Isso pode ocorrer com PySpark 4.0.0 no Windows. Certifique-se de usar PySpark 3.5.x:
```bash
pip install "pyspark>=3.5.0,<4.0.0"
```

### "O sistema nao pode encontrar o caminho especificado"
Verifique se esta na pasta correta:
```bash
cd DTM/DTM
dir main.py   # Windows
ls main.py    # Linux/Mac
```

### PostgreSQL: "connection refused"
O banco nao esta rodando. Inicie com:
```bash
docker-compose up -d
```

---

## Proximas paginas

- [Arquitetura do projeto](03-Arquitetura.md)
- [Referencia tecnica dos modulos](04-Referencia-Tecnica.md)
- [Seguranca e LGPD](05-Seguranca-e-LGPD.md)
