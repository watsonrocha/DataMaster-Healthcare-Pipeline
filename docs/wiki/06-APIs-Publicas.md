# APIs Publicas

O projeto utiliza duas APIs publicas reais para enriquecer o pipeline com dados verdadeiros. Nenhuma delas requer autenticacao (chave de API ou login).

---

## API do IBGE — Estados Brasileiros

### O que e?

A API do IBGE (Instituto Brasileiro de Geografia e Estatistica) fornece dados oficiais sobre a divisao territorial do Brasil.

### Endpoint utilizado

```
GET https://servicodados.ibge.gov.br/api/v1/localidades/estados
```

### Dados retornados

Para cada um dos **27 estados** brasileiros:

| Campo | Tipo | Exemplo | Descricao |
|-------|------|---------|-----------|
| `estado_id` | int | `35` | Codigo IBGE do estado |
| `sigla` | string | `SP` | Sigla do estado (2 letras) |
| `nome_estado` | string | `Sao Paulo` | Nome completo do estado |
| `regiao` | string | `Sudeste` | Regiao geografica |

### Exemplo de resposta processada

```json
[
    {
        "estado_id": 35,
        "sigla": "SP",
        "nome_estado": "São Paulo",
        "regiao": "Sudeste"
    },
    {
        "estado_id": 33,
        "sigla": "RJ",
        "nome_estado": "Rio de Janeiro",
        "regiao": "Sudeste"
    }
]
```

### Para que serve no projeto?

Os dados dos estados sao usados para:
- Enriquecer o Data Lake com dados geograficos reais do Brasil
- Demonstrar a integracao com APIs governamentais
- Podem ser cruzados com os dados de pacientes por estado

---

## API Disease.sh — Dados COVID-19

### O que e?

A **Disease.sh** e uma API gratuita que fornece dados epidemiologicos globais, incluindo casos, mortes e recuperacoes de COVID-19.

### Endpoints utilizados

#### 1. Dados atuais do Brasil

```
GET https://disease.sh/v3/covid-19/countries/Brazil
```

**Campos retornados:**

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `pais` | string | "Brazil" |
| `casos_total` | int | Total de casos confirmados |
| `mortes_total` | int | Total de obitos |
| `recuperados` | int | Total de recuperados |
| `ativos` | int | Casos ativos |
| `casos_por_milhao` | float | Casos por milhao de habitantes |
| `mortes_por_milhao` | float | Mortes por milhao de habitantes |
| `testes_total` | int | Total de testes realizados |
| `populacao` | int | Populacao estimada |
| `data_atualizacao` | string | Data/hora da ultima atualizacao |

#### 2. Historico diario (ultimos 30 dias)

```
GET https://disease.sh/v3/covid-19/historical/Brazil?lastdays=30
```

**Campos retornados (por dia):**

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `data` | string | Data no formato MM/DD/YYYY |
| `casos_acumulados` | int | Total de casos ate aquela data |
| `mortes_acumuladas` | int | Total de mortes ate aquela data |
| `recuperados_acumulados` | int | Total de recuperados ate aquela data |

### Exemplo de resposta processada (dados atuais)

```json
[
    {
        "pais": "Brazil",
        "casos_total": 38654271,
        "mortes_total": 713542,
        "recuperados": 37356044,
        "ativos": 584685,
        "casos_por_milhao": 178965.12,
        "mortes_por_milhao": 3305.89,
        "testes_total": 63776166,
        "populacao": 216009920,
        "data_atualizacao": "2026-05-03 14:30:00"
    }
]
```

---

## Tratamento de erros

As chamadas de API podem falhar por varios motivos (internet indisponivel, API fora do ar, etc.). O projeto trata esses erros de forma que o pipeline **nunca para** por causa de uma API:

```python
try:
    api_data["estados_ibge"] = fetch_ibge_estados()
except Exception as e:
    logger.warning("Falha ao buscar IBGE: %s", e)
    api_data["estados_ibge"] = []  # Continua com lista vazia
```

Se uma API falhar:
- O pipeline registra o erro no log (WARNING)
- Continua a execucao normalmente
- Os dados daquela API ficam vazios, mas as outras etapas nao sao afetadas

---

## Armazenamento dos dados de API

Os dados das APIs sao salvos em dois locais:

### 1. Data Lake (sempre)

Os dados sao salvos como arquivos JSON na camada **Bronze**:

```
output/data_lake/bronze/
├── api_estados_ibge.json       # 27 estados
├── api_covid_brasil.json       # Dados atuais COVID-19
└── api_covid_historico.json    # Historico 30 dias
```

### 2. PostgreSQL (se disponivel)

Se o PostgreSQL estiver rodando, os dados tambem sao inseridos nas tabelas:

| Tabela | Conteudo |
|--------|----------|
| `api_estados_ibge` | 27 estados com id, sigla, nome, regiao |
| `api_covid_brasil` | 1 registro com dados atuais do Brasil |
| `api_covid_historico` | ~30 registros (1 por dia) com historico |

---

## Configuracao

O timeout das requisicoes HTTP e configuravel:

```python
REQUEST_TIMEOUT = 15  # segundos
```

Se a API nao responder em 15 segundos, a requisicao e cancelada e o pipeline segue sem aqueles dados.

---

## Proximas paginas

- [Banco de Dados PostgreSQL](07-Banco-de-Dados.md)
- [Glossario](08-Glossario.md)
