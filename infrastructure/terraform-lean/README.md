# Stack AWS enxuto (S3 + RDS)

Versão de baixo custo da infraestrutura, para subir o projeto na AWS gastando
o mínimo (ideal para estudo / portfólio). Provisiona apenas:

- **S3** — buckets do Data Lake (`bronze/silver/gold`) e de checkpoints
- **RDS PostgreSQL** — `db.t3.micro` (elegível ao free tier), publicamente
  acessível para o pipeline gravar os dados Gold

Usa a **VPC default** da conta e **não** cria EMR, MSK nem NAT Gateway (os
recursos caros da stack completa em `../terraform`).

## Pré-requisitos

- Terraform >= 1.5
- Credenciais AWS com acesso a S3, RDS e EC2 (security groups)
- AWS CLI configurado (`aws sts get-caller-identity`)

## Como subir

```bash
cd infrastructure/terraform-lean

# senha forte para o banco (não fica versionada)
export TF_VAR_db_password="$(openssl rand -hex 16)"

terraform init
terraform plan  -var-file=lean.tfvars
terraform apply -var-file=lean.tfvars
```

Ao final, os outputs trazem o nome dos buckets e o endpoint do RDS.

## Rodar o pipeline contra a AWS

```bash
cd DTM/DTM
export DB_HOST="<rds_address dos outputs>"
export DB_PORT=5432 DB_NAME=healthcare DB_USER=pipeline
export DB_PASSWORD="$TF_VAR_db_password"
python main.py

# enviar o Data Lake local para o S3
aws s3 sync output/data_lake s3://<data_lake_bucket>/
```

## Custo

`db.t3.micro` + S3 ficam dentro do free tier no 1º ano. Fora do free tier, o
custo é de poucos dólares/mês. Ainda assim, **destrua quando não estiver usando**.

## Destruir tudo

```bash
cd infrastructure/terraform-lean
terraform destroy -var-file=lean.tfvars
```

> O `aws_s3_bucket` tem versionamento habilitado; se o `destroy` reclamar que o
> bucket não está vazio, esvazie antes com
> `aws s3 rm s3://<bucket> --recursive`.

## Segurança

`allowed_cidr` em `lean.tfvars` está como `0.0.0.0/0` (RDS aberto à internet)
para facilitar o estudo. Em qualquer uso sério, restrinja ao seu IP
(`SEU_IP/32`) e use uma senha forte.
