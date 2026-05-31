# Banco de Dados PostgreSQL

O projeto integra com o **PostgreSQL** para demonstrar o armazenamento de dados processados em um banco de dados relacional. Essa funcionalidade e **opcional** — o pipeline funciona normalmente sem o banco de dados.

---

## O que e o PostgreSQL?

O **PostgreSQL** (ou "Postgres") e um dos bancos de dados relacionais mais usados no mundo. Ele armazena dados em tabelas com linhas e colunas, semelhante a uma planilha do Excel, mas muito mais poderoso e confiavel.

---

## Como o projeto usa o PostgreSQL?

O pipeline salva os dados ja processados (camada **Gold**) e os dados das APIs publicas no PostgreSQL. Isso permite:

- Consultar dados com **SQL** (linguagem padrao de consulta a bancos de dados)
- Conectar ferramentas de **BI** (como Power BI, Tableau, Metabase)
- Demonstrar integracao completa: **API → Data Lake → Banco de Dados**

---

## Configuracao via Docker

O projeto inclui um arquivo `docker-compose.yml` que cria o banco automaticamente:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16           # Versao 16 do PostgreSQL
    container_name: healthcare_db
    environment:
      POSTGRES_DB: healthcare    # Nome do banco de dados
      POSTGRES_USER: pipeline    # Usuario
      POSTGRES_PASSWORD: pipeline123  # Senha
    ports:
      - "5432:5432"              # Porta de acesso
    volumes:
      - pgdata:/var/lib/postgresql/data  # Dados persistentes

volumes:
  pgdata:                        # Volume para manter dados entre reinicializacoes
```

### Iniciar o banco

```bash
# Na raiz do projeto
docker-compose up -d
```

O parametro `-d` faz o container rodar em segundo plano. Para verificar se esta rodando:

```bash
docker ps
# Deve mostrar o container "healthcare_db" como "Up"
```

### Parar o banco

```bash
docker-compose down        # Para o container (dados ficam salvos)
docker-compose down -v     # Para e APAGA todos os dados
```

---

## Tabelas do banco de dados

O pipeline cria automaticamente **5 tabelas** na primeira execucao:

### 1. gold_diagnostico

Dados agregados por diagnostico medico.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | SERIAL | Chave primaria (gerada automaticamente) |
| `diagnostico` | VARCHAR(100) | Nome do diagnostico (ex: "Hipertensao") |
| `total_pacientes` | INTEGER | Quantidade de pacientes com esse diagnostico |
| `media_freq_cardiaca` | FLOAT | Media da frequencia cardiaca |
| `media_sistolica` | FLOAT | Media da pressao sistolica |
| `media_diastolica` | FLOAT | Media da pressao diastolica |
| `media_temperatura` | FLOAT | Media da temperatura corporal |
| `created_at` | TIMESTAMP | Data/hora da insercao |

### 2. gold_estado

Dados agregados por estado brasileiro.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | SERIAL | Chave primaria |
| `estado` | VARCHAR(5) | Sigla do estado (ex: "SP") |
| `total_pacientes` | INTEGER | Total de pacientes no estado |
| `media_freq_cardiaca` | FLOAT | Media da frequencia cardiaca |
| `pacientes_pressao_alta` | INTEGER | Pacientes com pressao alta |
| `created_at` | TIMESTAMP | Data/hora da insercao |

### 3. api_estados_ibge

Dados dos estados brasileiros vindos da API do IBGE.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | SERIAL | Chave primaria |
| `estado_id` | INTEGER | Codigo IBGE do estado |
| `sigla` | VARCHAR(5) | Sigla (ex: "RJ") |
| `nome_estado` | VARCHAR(100) | Nome completo (ex: "Rio de Janeiro") |
| `regiao` | VARCHAR(50) | Regiao (ex: "Sudeste") |
| `created_at` | TIMESTAMP | Data/hora da insercao |

### 4. api_covid_brasil

Dados epidemiologicos do Brasil vindos da API Disease.sh.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | SERIAL | Chave primaria |
| `pais` | VARCHAR(50) | "Brazil" |
| `casos_total` | BIGINT | Total de casos confirmados |
| `mortes_total` | BIGINT | Total de obitos |
| `recuperados` | BIGINT | Total de recuperados |
| `ativos` | BIGINT | Casos ativos |
| `casos_por_milhao` | FLOAT | Casos por milhao de habitantes |
| `mortes_por_milhao` | FLOAT | Mortes por milhao |
| `testes_total` | BIGINT | Total de testes |
| `populacao` | BIGINT | Populacao estimada |
| `data_atualizacao` | TIMESTAMP | Data da ultima atualizacao dos dados |
| `created_at` | TIMESTAMP | Data/hora da insercao |

### 5. api_covid_historico

Historico diario de COVID-19 no Brasil (ultimos 30 dias).

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | SERIAL | Chave primaria |
| `data` | VARCHAR(20) | Data no formato MM/DD/YYYY |
| `casos_acumulados` | BIGINT | Casos acumulados ate a data |
| `mortes_acumuladas` | BIGINT | Mortes acumuladas ate a data |
| `recuperados_acumulados` | BIGINT | Recuperados acumulados |
| `created_at` | TIMESTAMP | Data/hora da insercao |

---

## Consultas SQL uteis

Apos executar o pipeline com PostgreSQL ativo, voce pode consultar os dados:

### Total de pacientes por diagnostico
```sql
SELECT diagnostico, total_pacientes, media_temperatura
FROM gold_diagnostico
ORDER BY total_pacientes DESC;
```

### Estados com mais pacientes com pressao alta
```sql
SELECT estado, total_pacientes, pacientes_pressao_alta
FROM gold_estado
ORDER BY pacientes_pressao_alta DESC;
```

### Regioes do Brasil (via IBGE)
```sql
SELECT regiao, COUNT(*) as total_estados
FROM api_estados_ibge
GROUP BY regiao
ORDER BY total_estados DESC;
```

### Dados gerais de COVID-19
```sql
SELECT pais, casos_total, mortes_total, recuperados,
       ROUND(mortes_total::numeric / casos_total * 100, 2) as taxa_mortalidade
FROM api_covid_brasil;
```

### Evolucao do COVID-19 nos ultimos 30 dias
```sql
SELECT data, casos_acumulados, mortes_acumuladas
FROM api_covid_historico
ORDER BY data;
```

### Para executar essas consultas

Via terminal:
```bash
docker exec -it healthcare_db psql -U pipeline -d healthcare
```

Isso abre um terminal interativo do PostgreSQL. Digite a consulta SQL e pressione Enter.

Para sair: `\q`

---

## Variaveis de ambiente do banco

| Variavel | Padrao | Descricao |
|----------|--------|-----------|
| `DB_HOST` | `localhost` | Host do PostgreSQL |
| `DB_PORT` | `5432` | Porta |
| `DB_NAME` | `healthcare` | Nome do banco |
| `DB_USER` | `pipeline` | Usuario |
| `DB_PASSWORD` | `pipeline123` | Senha |

---

## Proximas paginas

- [Glossario](08-Glossario.md)
- [Visao geral](01-Visao-Geral.md)
