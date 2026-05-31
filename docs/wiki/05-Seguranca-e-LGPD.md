# Seguranca e LGPD

Esta pagina explica como o projeto protege dados sensiveis de pacientes e implementa a conformidade com a **LGPD** (Lei Geral de Protecao de Dados).

---

## O que e a LGPD?

A **LGPD** (Lei no 13.709/2018) e a lei brasileira de protecao de dados pessoais. Ela exige que sistemas que processam dados de pessoas:

- **Protejam** dados sensiveis (CPF, nome, dados de saude)
- **Registrem** quem acessou os dados (auditoria)
- **Limitem** o acesso apenas a quem precisa (principio do menor privilegio)
- **Permitam** anonimizacao ou pseudonimizacao dos dados

O projeto implementa essas exigencias atraves de **mascaramento de dados** e **controle de acesso**.

---

## Mascaramento de Dados (data_masking.py)

O mascaramento transforma dados sensiveis de forma que nao seja possivel identificar a pessoa, mas o dado ainda seja util para analise.

### Tecnicas implementadas

| Campo | Tecnica | Exemplo (antes) | Exemplo (depois) |
|-------|---------|-----------------|-------------------|
| **CPF** | Exibicao parcial | `123.456.789-01` | `***456***` |
| **patient_id** | Hash SHA-256 | `PAC-847291` | `a1b2c3d4e5f6...` (64 caracteres) |
| **nome** | Pseudonimizacao | `Maria Silva` | `M***_a1b2c3` |
| **email** | Dominio preservado | `maria@gmail.com` | `m***@gmail.com` |
| **telefone** | Ultimos 4 digitos | `(11) 91234-5678` | `(**) *****-5678` |
| **outros** | Generico | `informacao` | `inf***` |

### Como funciona cada tecnica

#### CPF — Exibicao parcial
```python
# Entrada: "123.456.789-01"
# Saida:   "***456***"
# Apenas os 3 digitos centrais sao visiveis
```
**Util para**: verificar se dois registros sao do mesmo paciente sem expor o CPF completo.

#### patient_id — Hash SHA-256
```python
# Entrada: "PAC-847291"
# Saida:   "a1b2c3d4e5f67890..." (hash de 64 caracteres)
# O hash usa uma chave de criptografia (Config.ENCRYPTION_KEY)
# O mesmo patient_id sempre gera o mesmo hash (determinístico)
```
**Util para**: manter relacionamento entre registros sem expor o ID real.

#### nome — Pseudonimizacao
```python
# Entrada: "Maria Silva"
# Saida:   "M***_a1b2c3"
# Preserva a primeira letra + hash curto do nome
```
**Util para**: identificar parcialmente o paciente em relatorios internos.

#### email — Dominio preservado
```python
# Entrada: "maria.silva@hospital.com.br"
# Saida:   "m***@hospital.com.br"
# O dominio e preservado para analise de origem
```

#### telefone — Ultimos 4 digitos
```python
# Entrada: "(11) 91234-5678"
# Saida:   "(**) *****-5678"
# Apenas os ultimos 4 digitos sao visiveis
```

---

## Controle de Acesso — RBAC (access_control.py)

O **RBAC** (Role-Based Access Control) define o que cada tipo de usuario pode fazer no sistema.

### Perfis de usuario

| Perfil | Permissoes | Descricao |
|--------|-----------|-----------|
| **admin** | `read`, `write`, `delete`, `mask`, `export` | Acesso total ao pipeline e dados |
| **analista** | `read`, `write` | Pode ler e escrever dados processados |
| **cientista_dados** | `read`, `write`, `export` | Pode ler, escrever e exportar dados para analise |
| **visitante** | `read` | Apenas visualizacao de dados mascarados |

### Usuarios pre-cadastrados

| Usuario | Perfil |
|---------|--------|
| `admin` | admin |
| `analista_01` | analista |
| `cientista_01` | cientista_dados |
| `visitante_01` | visitante |

### Exemplo de verificacao de acesso

```python
access = AccessController()

# Verificar se usuario tem permissao
if access.has_permission("analista_01", "read"):
    print("Acesso permitido!")

# Exigir permissao (lanca erro se nao tiver)
access.require_permission("visitante_01", "delete")
# -> PermissionError: Usuario 'visitante_01' nao possui permissao 'delete'
```

---

## Log de Auditoria

Toda acao no sistema gera um registro de auditoria para conformidade:

```python
access.audit_log("analista_01", "read", "dados_pacientes")
# Resultado:
# {
#     "timestamp": "2026-05-03T17:38:59",
#     "user_id": "analista_01",
#     "role": "analista",
#     "action": "read",
#     "resource": "dados_pacientes",
#     "authorized": True
# }
```

Esses registros sao salvos no log do pipeline (`output/pipeline.log`).

---

## Criptografia

O projeto usa **SHA-256** para criar hashes de dados sensiveis. O SHA-256:

- **E irreversivel**: nao e possivel recuperar o dado original a partir do hash
- **E determinístico**: o mesmo dado sempre gera o mesmo hash
- **Usa uma chave**: a chave `ENCRYPTION_KEY` e combinada com o dado antes do hash, tornando-o unico para este projeto

> **Nota**: A chave padrao (`chave-segura-exemplo-2025`) e apenas para demonstracao. Em producao, use uma chave segura armazenada em um gerenciador de segredos.

---

## Resumo da conformidade LGPD

| Exigencia LGPD | Implementacao no projeto |
|-----------------|--------------------------|
| Protecao de dados sensiveis | Mascaramento automatico de CPF, nome, email, telefone, patient_id |
| Pseudonimizacao | Hash SHA-256 com chave para identificadores |
| Minimo privilegio | RBAC com 4 perfis de acesso diferenciados |
| Auditoria | Log de todas as acoes com timestamp, usuario, recurso |
| Portabilidade | Dados em formato padrao (Parquet, JSON, PostgreSQL) |

---

## Proximas paginas

- [APIs Publicas](06-APIs-Publicas.md)
- [Banco de Dados PostgreSQL](07-Banco-de-Dados.md)
