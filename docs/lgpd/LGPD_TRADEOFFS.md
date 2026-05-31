# LGPD — Trade-offs, Mascaramento, RBAC e Auditoria

> Documentação detalhada sobre decisões de conformidade com a Lei Geral de Proteção de Dados (LGPD - Lei 13.709/2018) aplicadas ao pipeline de dados de saúde.

---

## 1. Visão Geral da LGPD no Pipeline

O pipeline processa **dados pessoais sensíveis** (Art. 5º, II — dados relativos à saúde) e **dados pessoais** (CPF, nome, email, telefone), exigindo conformidade rigorosa com a LGPD.

### Bases Legais Utilizadas

| Base Legal | Artigo | Aplicação no Pipeline |
|---|---|---|
| Consentimento | Art. 7º, I | Dados de pacientes coletados com consentimento |
| Tutela da saúde | Art. 7º, VIII | Processamento para fins de saúde pública |
| Legítimo interesse | Art. 7º, IX | Análise agregada para melhoria de serviços |
| Proteção da vida | Art. 7º, VII | Alertas para condições críticas |

---

## 2. Estratégias de Mascaramento — Trade-offs

### 2.1 CPF — Exibição Parcial

```
Original: 123.456.789-00
Mascarado: ***456***
```

| Aspecto | Decisão | Trade-off |
|---|---|---|
| **Técnica** | Exibição parcial (3 dígitos centrais) | Preserva utilidade para conferência parcial |
| **Reversibilidade** | Irreversível sem chave | Impossível reconstruir o CPF completo |
| **Utilidade** | Permite identificação parcial pelo titular | Não permite validação algorítmica (dígitos verificadores perdidos) |
| **Risco residual** | Baixo — 3 dígitos não identificam univocamente | Combinação com outros dados poderia aumentar risco |

**Alternativas consideradas:**
- Hash completo (SHA-256): mais seguro, mas perde utilidade para conferência
- Tokenização reversível: requer gestão de tokens, mas permite destokenização autorizada
- Criptografia AES: reversível com chave, mas exige gestão segura de chaves

**Recomendação para produção:** Tokenização com vault (ex: HashiCorp Vault) para permitir destokenização auditada quando necessário.

### 2.2 Nome — Pseudonimização

```
Original: Maria Silva
Mascarado: M***_a3f2c1
```

| Aspecto | Decisão | Trade-off |
|---|---|---|
| **Técnica** | Inicial + hash parcial SHA-256 | Preserva inicial para agrupamento |
| **Reversibilidade** | Irreversível | Hash parcial não permite reconstrução |
| **Utilidade** | Permite agrupamento aproximado | Perde capacidade de endereçamento |
| **Risco residual** | Médio — inicial + contexto pode identificar | Combinação com cidade/estado aumenta risco |

### 2.3 Email — Domínio Preservado

```
Original: maria@hospital.com.br
Mascarado: m***@hospital.com.br
```

| Aspecto | Decisão | Trade-off |
|---|---|---|
| **Técnica** | Preserva domínio, mascara local part | Permite análise por domínio/instituição |
| **Utilidade** | Análise de distribuição por instituição | Domínio pode ser informação sensível |
| **Risco** | Domínio institucional pode identificar empregador | Aceitável para análise de rede de saúde |

### 2.4 Patient ID — Hash SHA-256 com Chave

```
Original: PAC-123456
Mascarado: a3f2c1d4e5... (64 caracteres)
```

| Aspecto | Decisão | Trade-off |
|---|---|---|
| **Técnica** | HMAC-SHA-256 com chave secreta | Permite linkabilidade com mesma chave |
| **Reversibilidade** | Irreversível sem enumeração de IDs | Seguro contra rainbow tables |
| **Utilidade** | Mantém capacidade de JOIN entre tabelas | Exige mesma chave em todo o pipeline |
| **Risco** | Comprometimento da chave expõe mapeamento | Rotação de chave quebra linkabilidade histórica |

**Rotação de chaves:** Implementar rotação trimestral com período de overlap (ambas as chaves ativas por 30 dias).

### 2.5 Telefone — Últimos 4 Dígitos

```
Original: (11) 91234-5678
Mascarado: (**) *****-5678
```

| Aspecto | Decisão | Trade-off |
|---|---|---|
| **Técnica** | Preserva últimos 4 dígitos | Permite verificação parcial pelo titular |
| **Risco** | 4 dígitos = 10.000 combinações | Combinação com DDD mascarado reduz risco |

---

## 3. Controle de Acesso (RBAC) — Design e Trade-offs

### 3.1 Modelo de Papéis

```
admin ──────────── read, write, delete, mask, export
  │
analista ───────── read, write
  │
cientista_dados ── read, write, export
  │
visitante ──────── read
```

### 3.2 Trade-offs do Modelo

| Decisão | Benefício | Limitação |
|---|---|---|
| 4 papéis fixos | Simples de gerenciar | Pouca granularidade |
| Permissões por ação | Controle claro | Não controla escopo (quais dados) |
| Sem hierarquia | Evita escalonamento acidental | Duplicação de permissões |
| In-memory | Sem dependência externa | Não persiste entre sessões |

### 3.3 Recomendações para Produção

1. **ABAC (Attribute-Based Access Control):** Adicionar controle por atributo dos dados (ex: analista só vê dados da região X).
2. **Integração com IdP:** LDAP/SAML/OIDC para gestão centralizada de identidades.
3. **Least Privilege:** Cada job Spark deve rodar com o mínimo de permissões necessárias.
4. **Row-Level Security:** Filtrar registros baseado no papel do usuário na query.

```python
# Exemplo de ABAC proposto
class ABACPolicy:
    def evaluate(self, user, action, resource, context):
        rules = [
            # Analistas da região só veem dados da sua região
            Rule(role="analista", action="read",
                 condition=lambda ctx: ctx["user_region"] == ctx["data_region"]),
            # Cientistas podem exportar dados mascarados
            Rule(role="cientista_dados", action="export",
                 condition=lambda ctx: ctx["data_masked"] is True),
        ]
```

---

## 4. Auditoria — Design e Conformidade

### 4.1 Campos do Log de Auditoria

```json
{
    "timestamp": "2025-01-15T10:30:00.123Z",
    "user_id": "admin",
    "role": "admin",
    "action": "read",
    "resource": "healthcare_batch",
    "authorized": true,
    "ip_address": "10.0.1.50",
    "session_id": "sess-abc123"
}
```

### 4.2 Requisitos LGPD para Auditoria

| Requisito LGPD | Implementação | Status |
|---|---|---|
| Art. 37 — Registro de operações | Log de auditoria em cada acesso | Implementado |
| Art. 38 — Relatório de impacto | Histórico de operações via Delta time travel | Implementado |
| Art. 46 — Medidas de segurança | SHA-256, RBAC, mascaramento | Implementado |
| Art. 48 — Comunicação de incidentes | Alertas Prometheus para acessos anormais | Implementado |
| Art. 49 — Privacy by Design | Mascaramento na camada Silver | Implementado |

### 4.3 Trade-offs da Auditoria

| Decisão | Benefício | Limitação |
|---|---|---|
| Log em arquivo | Simples, sem dependência | Difícil de consultar em escala |
| Log por operação | Granularidade fina | Volume alto de registros |
| Sem retenção automática | Nada é perdido | Pode violar princípio de minimização |

### 4.4 Recomendações para Produção

1. **Log centralizado:** AWS CloudWatch Logs ou ELK Stack para consulta e alertas.
2. **Retenção definida:** 5 anos para saúde (CFM) + purge automático após período.
3. **Imutabilidade:** Escrever logs em S3 com Object Lock (WORM).
4. **Alertas:** Tentativas de acesso negado devem gerar alerta imediato.

---

## 5. Direitos do Titular (LGPD Arts. 17-22)

### 5.1 Implementação dos Direitos

| Direito | Artigo | Implementação |
|---|---|---|
| Acesso | Art. 18, II | `read_layer("silver", patient_filter)` |
| Correção | Art. 18, III | `merge(corrected_data, ...)` com SCD Type 2 |
| Eliminação | Art. 18, VI | Soft delete (is_deleted=true) + vacuum após retenção |
| Portabilidade | Art. 18, V | Export para CSV/JSON dos dados do titular |
| Revogação de consentimento | Art. 18, IX | Flag de consentimento + reprocessamento |

### 5.2 Direito ao Esquecimento — Trade-offs

```
Abordagem 1: Hard Delete
  + Garantia total de eliminação
  - Quebra consistência de agregados Gold
  - Não é possível em formatos append-only

Abordagem 2: Soft Delete + Crypto Erasure
  + Preserva agregados
  + Compatível com Delta Lake
  - Dados cifrados permanecem (sem chave)
  
Abordagem 3: Tokenização + Delete Token
  + Mais elegante
  + Mantém integridade referencial
  - Complexidade operacional
```

**Decisão atual:** Soft delete com flag `is_deleted` + `vacuum()` periódico para remoção física.

---

## 6. Classificação de Dados por Camada

| Camada | Dados Pessoais | Mascaramento | Acesso Permitido |
|---|---|---|---|
| **Bronze** | Sim (dados brutos) | Nenhum | Somente admin |
| **Silver** | Sim (mascarados) | CPF, nome, email, tel, patient_id | admin, analista |
| **Gold** | Não (agregados) | N/A | Todos os papéis |

### Privacy by Design

O mascaramento ocorre na transição **Bronze -> Silver**, garantindo que dados pessoais em claro existam apenas na camada Bronze com acesso restrito.

---

## 7. Matriz de Riscos LGPD

| Risco | Probabilidade | Impacto | Mitigação |
|---|---|---|---|
| Vazamento de dados Bronze | Baixa | Alto | Criptografia S3 + ACL restritiva |
| Re-identificação via Silver | Média | Médio | Mascaramento robusto + minimização de campos |
| Acesso não autorizado | Baixa | Alto | RBAC + auditoria + alertas |
| Perda de dados | Baixa | Alto | Backup + versionamento S3 + Delta time travel |
| Não-conformidade regulatória | Média | Alto | Documentação + DPO + relatório de impacto |

---

## 8. Custos de Conformidade

| Componente | Custo (mensal estimado) | Justificativa |
|---|---|---|
| KMS (chaves) | ~$1/chave | Rotação automática |
| CloudWatch Logs | ~$0.50/GB ingerido | Logs de auditoria |
| S3 Object Lock | +20% sobre storage | Imutabilidade para auditoria |
| DPO (pessoa) | Variável | Obrigatório para dados de saúde |
| Treinamento | Variável | LGPD awareness para equipe |

---

## Referências

- [LGPD — Lei 13.709/2018](https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm)
- [ANPD — Guia Orientativo sobre Tratamento de Dados de Saúde](https://www.gov.br/anpd/pt-br)
- [CFM — Resolução 2.217/2018 (Código de Ética Médica)](https://www.cfm.org.br)
- [ISO 27001 — Information Security Management](https://www.iso.org/isoiec-27001-information-security.html)
- [NIST Privacy Framework](https://www.nist.gov/privacy-framework)
