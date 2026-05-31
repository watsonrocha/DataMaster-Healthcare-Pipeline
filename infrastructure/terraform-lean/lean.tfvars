aws_region  = "us-east-1"
environment = "dev"
db_username = "pipeline"

# Restrinja a um IP/CIDR específico em produção. 0.0.0.0/0 abre o RDS
# para a internet (use apenas com senha forte e em ambiente de estudo).
allowed_cidr = "0.0.0.0/0"
