# Visao Geral do Projeto

## O que e este projeto?

O **DataMasterFinal** e um pipeline de processamento de dados de saude desenvolvido com **PySpark**. Ele simula um cenario real de engenharia de dados em que dados de pacientes sao extraidos de diversas fontes, transformados, protegidos e armazenados de forma organizada.

Em termos simples: imagine um sistema hospitalar que precisa coletar dados de milhares de pacientes vindos de diferentes fontes (planilhas, APIs, banco de dados), limpar e organizar esses dados, proteger informacoes sensiveis (como CPF e nome) e gerar relatorios para analise.

## Para que serve?

O projeto atende **8 requisitos** de engenharia de dados:

| # | Requisito | O que faz |
|---|-----------|-----------|
| 1 | **Extracao de Dados** | Gera dados simulados de pacientes e busca dados reais de APIs publicas (IBGE e COVID-19) |
| 2 | **Ingestao de Dados** | Le dados de arquivos CSV e JSON, e simula ingestao em tempo real (streaming) |
| 3 | **Armazenamento** | Salva os dados em 3 camadas organizadas (Bronze, Silver, Gold) + PostgreSQL |
| 4 | **Observabilidade** | Monitora cada etapa do pipeline: tempo de execucao, qualidade dos dados, alertas |
| 5 | **Seguranca** | Controla quem pode acessar os dados (RBAC) e gera logs de auditoria |
| 6 | **Mascaramento** | Protege dados sensiveis (CPF, nome, email, telefone) conforme a LGPD |
| 7 | **Arquitetura** | Usa o padrao Medallion (Bronze/Silver/Gold) com formato Parquet otimizado |
| 8 | **Escalabilidade** | Funciona localmente e pode escalar para clusters Spark |

## Tecnologias utilizadas

| Tecnologia | Versao | Para que serve |
|-----------|--------|----------------|
| **Python** | 3.10+ | Linguagem principal do projeto |
| **PySpark** | 3.5.x | Framework de processamento distribuido de dados |
| **Java** | 11-21 (recomendado: 17) | Necessario para rodar o Spark (motor interno) |
| **PostgreSQL** | 16 | Banco de dados relacional para armazenar resultados (opcional) |
| **Docker** | 20+ | Facilita a execucao do PostgreSQL (opcional) |
| **Faker** | 25+ | Gera dados ficticios realistas (nomes, CPFs, enderecos brasileiros) |
| **Requests** | 2.31+ | Faz chamadas HTTP para as APIs publicas |
| **psycopg2** | 2.9+ | Driver Python para conectar ao PostgreSQL |

## Fluxo resumido do pipeline

```
   EXTRAÇÃO               INGESTÃO            ARMAZENAMENTO
┌──────────────┐    ┌─────────────────┐    ┌────────────────┐
│ Dados Faker  │───>│ Leitura CSV     │───>│ BRONZE         │
│ API IBGE     │    │ Leitura JSON    │    │ (dados brutos) │
│ API COVID-19 │    │ Streaming sim.  │    │                │
└──────────────┘    └─────────────────┘    └───────┬────────┘
                                                    │
                     TRANSFORMAÇÃO                   │
                    ┌─────────────────┐              │
                    │ Limpeza         │<─────────────┘
                    │ Enriquecimento  │
                    │ Mascaramento    │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │ SILVER          │
                    │ (dados limpos)  │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │ GOLD            │───> PostgreSQL
                    │ (agregacoes)    │     (opcional)
                    └─────────────────┘
```

## Proximos passos

- [Como instalar e executar](02-Instalacao-e-Execucao.md)
- [Arquitetura detalhada](03-Arquitetura.md)
- [Referencia tecnica dos modulos](04-Referencia-Tecnica.md)
