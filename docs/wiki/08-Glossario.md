# Glossario

Termos tecnicos usados no projeto, explicados de forma simples.

---

## A

### API (Application Programming Interface)
Uma "ponte" que permite que um programa consulte dados de outro sistema pela internet. No projeto, usamos APIs do IBGE e Disease.sh para buscar dados reais.

### Apache Parquet
Formato de arquivo otimizado para grandes volumes de dados. Armazena dados em colunas (ao inves de linhas), o que torna a leitura muito mais rapida quando voce precisa de apenas algumas colunas.

### Apache Spark / PySpark
Framework de processamento de dados em grande escala. O **PySpark** e a versao em Python do Spark. Ele permite processar milhoes de registros de forma distribuida (dividindo o trabalho entre multiplos processadores).

---

## B

### Batch (processamento em lote)
Processamento de dados que ja foram coletados. Ao contrario do streaming, os dados nao chegam em tempo real — eles ja estao armazenados em arquivos (CSV, JSON) e sao processados de uma vez.

### Bronze (camada)
Primeira camada do Data Lake. Contem os dados **brutos**, exatamente como foram recebidos, sem nenhuma transformacao. Serve como backup e fonte de verdade.

---

## C

### CSV (Comma-Separated Values)
Formato de arquivo de texto simples em que cada linha e um registro e as colunas sao separadas por virgulas. Pode ser aberto no Excel.

### CPF (Cadastro de Pessoa Fisica)
Numero de identificacao de pessoas no Brasil. E considerado dado sensivel pela LGPD e precisa ser protegido.

---

## D

### Data Lake
Sistema de armazenamento que guarda grandes volumes de dados em formatos variados (CSV, JSON, Parquet). Diferente de um banco de dados tradicional, o Data Lake aceita dados brutos e estruturados juntos.

### DataFrame
Estrutura de dados do PySpark semelhante a uma tabela. Tem linhas (registros) e colunas (campos), como uma planilha do Excel. E a principal unidade de trabalho no pipeline.

### Docker
Ferramenta que cria "containers" — ambientes isolados onde programas rodam sem afetar o seu computador. No projeto, usamos Docker para rodar o PostgreSQL sem precisar instala-lo.

### docker-compose
Ferramenta que define e gerencia multiplos containers Docker com um unico arquivo de configuracao (`docker-compose.yml`).

---

## E

### ETL (Extract, Transform, Load)
Padrao de processamento de dados:
- **Extract**: extrair dados das fontes (APIs, arquivos, bancos)
- **Transform**: limpar, enriquecer e transformar os dados
- **Load**: carregar os dados processados no destino (Data Lake, banco de dados)

---

## F

### Faker
Biblioteca Python que gera dados ficticios realistas. No projeto, gera nomes, CPFs, enderecos e emails brasileiros.

---

## G

### Gold (camada)
Terceira e ultima camada do Data Lake. Contem dados **agregados** e prontos para consumo por ferramentas de BI (Business Intelligence). Exemplo: "total de pacientes por diagnostico".

---

## H

### Hadoop
Framework para processamento distribuido de dados. O PySpark usa internamente componentes do Hadoop (como o HDFS). No Windows, e necessario o arquivo `winutils.exe` para compatibilidade.

### Hash (SHA-256)
Funcao matematica que transforma qualquer texto em uma sequencia fixa de 64 caracteres hexadecimais. E irreversivel (nao da para "desfazer"). Usado para proteger identificadores de pacientes.

---

## J

### Java / JDK
Linguagem de programacao e plataforma necessaria para rodar o Apache Spark. O projeto requer Java 17 (JDK = Java Development Kit).

### JAVA_HOME
Variavel de ambiente do sistema operacional que indica onde o Java esta instalado. O PySpark precisa dessa variavel para encontrar o Java.

### JDBC (Java Database Connectivity)
Protocolo padrao do Java para conectar a bancos de dados. O PySpark pode usar JDBC para ler/escrever no PostgreSQL.

### JSON (JavaScript Object Notation)
Formato de dados leve e legivel. Exemplo: `{"nome": "Maria", "idade": 30}`. Muito usado em APIs e troca de dados entre sistemas.

### JSON Lines (JSONL)
Variacao do JSON onde cada linha do arquivo e um objeto JSON independente. Facilita o processamento de grandes volumes de dados.

---

## K

### Kafka
Plataforma de streaming de dados em tempo real. O projeto suporta Kafka opcionalmente, mas usa simulacao quando nao esta disponivel.

---

## L

### LGPD (Lei Geral de Protecao de Dados)
Lei brasileira (no 13.709/2018) que regulamenta o tratamento de dados pessoais. Exige protecao de dados sensiveis como CPF, nome e dados de saude.

---

## M

### Mascaramento
Tecnica de protecao de dados que substitui informacoes sensiveis por versoes parciais ou criptografadas. Exemplo: CPF `123.456.789-01` vira `***456***`.

### Medallion Architecture
Padrao de organizacao de Data Lake em 3 camadas: Bronze (bruto), Silver (limpo) e Gold (agregado). Tambem chamado de "Multi-Hop Architecture".

---

## P

### Parquet
Ver **Apache Parquet**.

### Pipeline
Sequencia automatizada de etapas que processam dados do inicio ao fim. Neste projeto: extracao → ingestao → transformacao → armazenamento.

### PostgreSQL
Banco de dados relacional de codigo aberto. Armazena dados em tabelas com schema definido (colunas tipadas). Usado no projeto para armazenar dados processados.

### Pseudonimizacao
Tecnica de protecao de dados que substitui identificadores diretos por codigos ou hashes. O dado original nao e visivel, mas um mesmo paciente sempre recebe o mesmo pseudonimo.

---

## R

### RBAC (Role-Based Access Control)
Modelo de seguranca onde permissoes sao atribuidas a **perfis** (roles), nao a usuarios individuais. Um usuario herda as permissoes do seu perfil.

---

## S

### Schema
Definicao da estrutura dos dados: quais colunas existem, que tipo cada uma tem (texto, numero, data, etc.).

### Silver (camada)
Segunda camada do Data Lake. Contem dados **limpos e validados**: duplicatas removidas, campos enriquecidos (como a categorizacao de pressao arterial) e dados sensiveis mascarados.

### Spark / SparkSession
Ver **Apache Spark**. A **SparkSession** e o objeto principal que controla a conexao com o Spark e permite processar dados.

### Streaming
Processamento de dados em tempo real, conforme eles chegam. No projeto, o streaming e **simulado** (gera dados aleatorios que imitam dados chegando em tempo real).

---

## U

### UDF (User Defined Function)
Funcao personalizada do PySpark que o usuario define para aplicar transformacoes complexas nos dados.

---

## W

### winutils.exe
Utilitario do Hadoop necessario para rodar PySpark no Windows. O projeto baixa automaticamente esse arquivo do GitHub na primeira execucao.

---

## Indice de paginas

1. [Visao geral do projeto](01-Visao-Geral.md)
2. [Instalacao e execucao](02-Instalacao-e-Execucao.md)
3. [Arquitetura do projeto](03-Arquitetura.md)
4. [Referencia tecnica dos modulos](04-Referencia-Tecnica.md)
5. [Seguranca e LGPD](05-Seguranca-e-LGPD.md)
6. [APIs publicas](06-APIs-Publicas.md)
7. [Banco de dados PostgreSQL](07-Banco-de-Dados.md)
8. [Glossario](08-Glossario.md)
