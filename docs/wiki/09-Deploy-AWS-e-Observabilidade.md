# Deploy na Nuvem AWS e Observabilidade

Esta pagina documenta o **deploy real do projeto na nuvem AWS** e a **observabilidade** (monitoramento) com Prometheus e Grafana. Tudo o que esta descrito aqui foi efetivamente provisionado e validado.

---

## 1. O projeto esta na nuvem? Sim — modelo hibrido

O projeto roda hoje em um modelo **hibrido (on-premise + cloud)**:

- **Na nuvem (AWS)** fica o **armazenamento** dos dados:
  - **Amazon S3** = Data Lake (camadas Bronze / Silver / Gold em Parquet)
  - **Amazon RDS PostgreSQL** = banco com as tabelas Gold (dados de negocio prontos para consumo)
- **Local (sua maquina, via Docker)** fica o **processamento** e as ferramentas:
  - O pipeline PySpark (ETL), o Airflow, o Prometheus e o Grafana rodam em containers

Em outras palavras: **os dados moram na AWS**, mas o **motor que processa** roda na sua maquina. Voce pode desligar o PC que o S3 e o RDS continuam la.

```
   FONTES                PROCESSAMENTO (local, Docker)         ARMAZENAMENTO (AWS)
┌───────────┐         ┌──────────────────────────────┐      ┌──────────────────┐
│ Pacientes │         │  BRONZE  → SILVER  → GOLD      │ ───► │  S3 (Data Lake)  │
│ APIs      │ ──────► │  + LGPD (mascara CPF/nome)     │      │  Parquet         │
│ (IBGE,    │         │  + Qualidade de dados          │      ├──────────────────┤
│  COVID)   │         │  + Metricas (Prometheus)       │ ───► │  RDS PostgreSQL  │
└───────────┘         └──────────────────────────────┘      │  tabelas Gold    │
                                                             └──────────────────┘
                                                                      │
                                                             ┌────────┴────────┐
                                                             │ Grafana le e    │
                                                             │ mostra em       │
                                                             │ dashboards      │
                                                             └─────────────────┘
```

---

## 2. Infraestrutura como Codigo (Terraform)

Toda a infraestrutura da AWS foi criada com **Terraform** — uma ferramenta de **IaC (Infrastructure as Code)**. Em vez de clicar no console da AWS, descrevemos os recursos em arquivos `.tf` e criamos tudo com um comando.

Por que IaC?
- **Reproduzivel**: qualquer pessoa sobe a mesma infraestrutura identica.
- **Versionado**: esta no Git, com historico de mudancas.
- **Auditavel**: da para revisar o que sera criado (`terraform plan`) antes de aplicar.
- **Descartavel**: `terraform destroy` remove tudo, evitando custo.

### Stack enxuto (o que foi realmente deployado)

Para provar o projeto na nuvem com **custo proximo de zero**, usamos uma versao enxuta da infraestrutura, em `infrastructure/terraform-lean/`:

```
infrastructure/terraform-lean/
├── main.tf          # S3 (Data Lake) + RDS PostgreSQL + security group
├── variables.tf     # Variaveis configuraveis (regiao, senha, etc.)
├── outputs.tf       # Endpoints e nomes dos recursos criados
├── lean.tfvars      # Valores de configuracao (regiao us-east-1, etc.)
└── README.md        # Instrucoes de uso
```

Comandos usados para subir:

```bash
cd infrastructure/terraform-lean
export TF_VAR_db_password="<senha-do-banco>"   # nunca commitada
terraform init
terraform plan  -var-file=lean.tfvars
terraform apply -var-file=lean.tfvars
```

Para destruir e zerar o custo:

```bash
terraform destroy -var-file=lean.tfvars
```

> A senha do banco vai pela variavel de ambiente `TF_VAR_db_password` e nunca fica no codigo. O arquivo de estado (`.tfstate`) esta no `.gitignore`.

---

## 3. O que esta rodando na AWS

| Recurso | Detalhe |
|---|---|
| **S3 Data Lake** | Bucket `healthcare-datalake-<account>-dev` com Bronze / Silver / Gold em Parquet (lifecycle configurado) |
| **S3 Checkpoints** | Bucket `healthcare-datalake-<account>-checkpoints-dev` (checkpoints do streaming) |
| **RDS PostgreSQL 16** | Instancia `db.t3.micro` (Free Tier), banco `healthcare`, tabelas Gold |
| **Security Group** | Regra de rede que libera o acesso ao RDS na porta 5432 |
| **Regiao** | `us-east-1` (N. Virginia) |

Tabelas gravadas no RDS na nuvem: `gold_diagnostico`, `gold_estado`, `api_estados_ibge`, `api_covid_brasil`, `api_covid_historico`.

---

## 4. Decisao de custo (importante para a banca)

Existem **duas versoes** de infraestrutura no projeto:

| Versao | Recursos | Custo aproximado | Quando usar |
|---|---|---|---|
| **Completa** (`infrastructure/terraform/`) | VPC + NAT + S3 + **EMR** (Spark gerenciado) + **MSK** (Kafka gerenciado) + RDS | ~US$ 400/mes | Producao real, alto volume |
| **Enxuta** (`infrastructure/terraform-lean/`) | S3 + RDS (+ security group) | ~US$ 0 (Free Tier) | Provar o projeto na nuvem, portfolio, estudo |

Escolhemos a versao **enxuta** porque ela coloca na nuvem o que importa (os dados) gastando praticamente nada. A arquitetura continua a mesma; a migracao para a versao gerenciada e direta (os modulos Terraform de EMR e MSK ja existem no repo). Mostrar essa analise demonstra maturidade: **nuvem nao e "ligar tudo", e escolher o que faz sentido**.

---

## 5. Observabilidade: dois tipos de monitoramento

O projeto tem **dois tipos de dashboard** no Grafana, que respondem perguntas diferentes:

### a) Monitoramento operacional (Prometheus -> Grafana)

Responde: **"o pipeline esta saudavel?"**

O pipeline exporta metricas em tempo real na porta `8000` (`/metrics`). O Prometheus coleta e o Grafana exibe.

Metricas expostas:
- `pipeline_records_processed_total` — registros processados
- `pipeline_stage_duration_seconds` — duracao de cada etapa
- `pipeline_data_quality_null_ratio` — proporcao de nulos (qualidade)
- `pipeline_errors_total` — erros
- `pipeline_streaming_lag_seconds` — atraso do streaming

### b) Dados de negocio na nuvem (Grafana -> RDS AWS)

Responde: **"o que os dados dizem?"**

Configuramos no Grafana um **datasource PostgreSQL** apontando direto para o **RDS na AWS** (provisionado automaticamente via YAML). O dashboard **"Healthcare Gold — RDS (AWS)"** roda SQL nas tabelas Gold e mostra:
- **220 pacientes** (total)
- **11 diagnosticos** distintos
- Pacientes por diagnostico (grafico de barras)
- Sinais vitais medios por diagnostico (tabela detalhe)

Separar observabilidade **operacional** de analytics de **negocio** e uma boa pratica de engenharia de dados.

---

## 6. Como o Grafana se conecta ao RDS (sem expor senha)

A senha do RDS **nao fica no codigo**. Ela e lida de um arquivo `.env` (que esta no `.gitignore`) via variavel de ambiente:

Arquivo `.env` na raiz do projeto:

```
RDS_HOST=<endpoint-do-rds>.us-east-1.rds.amazonaws.com
RDS_DB=healthcare
RDS_USER=pipeline
RDS_PASSWORD=<senha>
```

Subir a stack e recriar o Grafana para ele ler a senha:

```bash
docker compose -f docker-compose.prod.yml up -d
# se mudar o .env depois, recrie so o Grafana:
docker compose -f docker-compose.prod.yml up -d --force-recreate grafana
```

Depois: Grafana em `http://localhost:3000` → Dashboards → **"Healthcare Gold — RDS (AWS)"**.

---

## 7. Perguntas provaveis da banca

**O seu projeto esta na nuvem?**
Sim, num modelo hibrido: o armazenamento (S3 + RDS) esta na AWS e o processamento roda local (Docker). Tudo provisionado com Terraform.

**Por que S3 + RDS e nao EMR + MSK?**
Por decisao de custo. A stack completa custaria ~US$ 400/mes; a enxuta prova o projeto na nuvem dentro do Free Tier (~US$ 0). A migracao para a versao gerenciada e direta.

**O que e Terraform?**
Ferramenta de Infraestrutura como Codigo: descreve os recursos da AWS em arquivos versionados e cria tudo com `terraform apply` — reproduzivel, auditavel e descartavel.

**Como garante que credenciais nao vazam?**
Senha do RDS via `.env` (no `.gitignore`); senha do Terraform via `TF_VAR_db_password`; estado `.tfstate` fora do Git; credenciais AWS em variaveis de ambiente. Proximo passo em producao: AWS Secrets Manager / KMS.

**O que acontece se o RDS ficar indisponivel?**
O pipeline tem fallback gracioso: ele continua gravando o Data Lake (S3/local) mesmo sem o banco. O dashboard de negocio do Grafana ficaria sem dados ate o RDS voltar, mas o monitoramento operacional (Prometheus) continua funcionando.

---

## Resumo

> O projeto esta deployado na AWS num modelo hibrido: Data Lake no S3 e tabelas Gold no RDS PostgreSQL, tudo provisionado por Terraform (versao enxuta, ~US$ 0 no Free Tier). A observabilidade tem dois niveis: metricas operacionais do pipeline (Prometheus + Grafana) e dados de negocio lidos direto do RDS na nuvem (dashboard "Healthcare Gold — RDS").
