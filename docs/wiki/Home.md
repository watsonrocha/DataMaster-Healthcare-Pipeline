# DataMasterFinal — Wiki

Bem-vindo a documentacao completa do projeto **DataMasterFinal**!

Este projeto e um pipeline de engenharia de dados de saude desenvolvido com PySpark. A documentacao foi criada para que **qualquer pessoa** consiga entender e executar o projeto.

---

## Indice

### Para comecar
1. **[Visao Geral](01-Visao-Geral.md)** — O que e o projeto, para que serve, tecnologias usadas
2. **[Instalacao e Execucao](02-Instalacao-e-Execucao.md)** — Passo a passo para instalar e rodar no Windows, Linux ou Mac

### Entendendo o projeto
3. **[Arquitetura](03-Arquitetura.md)** — Como o projeto e organizado (Medallion Architecture, fluxo de dados)
4. **[Referencia Tecnica](04-Referencia-Tecnica.md)** — Descricao detalhada de cada modulo, classe e funcao

### Funcionalidades
5. **[Seguranca e LGPD](05-Seguranca-e-LGPD.md)** — Mascaramento de dados, RBAC, auditoria, conformidade
6. **[APIs Publicas](06-APIs-Publicas.md)** — IBGE e Disease.sh: endpoints, dados retornados, tratamento de erros
7. **[Banco de Dados PostgreSQL](07-Banco-de-Dados.md)** — Docker, tabelas, consultas SQL uteis

### Referencia
8. **[Glossario](08-Glossario.md)** — Termos tecnicos explicados de forma simples

### Nuvem e Observabilidade
9. **[Deploy na Nuvem AWS e Observabilidade](09-Deploy-AWS-e-Observabilidade.md)** — Deploy real na AWS (S3 + RDS via Terraform), modelo hibrido, decisao de custo, Grafana lendo o RDS

---

## Links rapidos

- **Repositorio**: https://github.com/watsonrocha/DataMasterFinal
- **Executar o pipeline**: `cd DTM/DTM && python main.py`
- **Instalar dependencias**: `pip install -r requirements.txt`
- **Iniciar PostgreSQL**: `docker-compose up -d`
