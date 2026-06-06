#!/usr/bin/env python3
"""
Sincroniza dados Gold do PostgreSQL local (Docker) para o RDS na AWS.

Uso:
  export RDS_HOST="healthcare-lean-dev.xxxxx.us-east-1.rds.amazonaws.com"
  export RDS_PASSWORD="<senha>"
  python scripts/sync_gold_to_rds.py

O script lê as tabelas gold_diagnostico, gold_estado, api_estados_ibge,
api_covid_brasil e api_covid_historico do banco local e insere no RDS.
"""

import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 não instalado. Execute: pip install psycopg2-binary")
    sys.exit(1)

# ── Configuração local (Docker) ──────────────────────────────────────
LOCAL = {
    "host": os.getenv("LOCAL_DB_HOST", "localhost"),
    "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
    "database": os.getenv("LOCAL_DB_NAME", "healthcare"),
    "user": os.getenv("LOCAL_DB_USER", "pipeline"),
    "password": os.getenv("LOCAL_DB_PASSWORD", "pipeline123"),
}

# ── Configuração RDS (AWS) ────────────────────────────────────────────
RDS = {
    "host": os.getenv("RDS_HOST", "healthcare-lean-dev.c8xg4i8k0ii9.us-east-1.rds.amazonaws.com"),
    "port": int(os.getenv("RDS_PORT", "5432")),
    "database": os.getenv("RDS_DB", "healthcare"),
    "user": os.getenv("RDS_USER", "pipeline"),
    "password": os.getenv("RDS_PASSWORD", ""),
    "sslmode": "require",
}

TABLES = [
    "gold_diagnostico",
    "gold_estado",
    "api_estados_ibge",
    "api_covid_brasil",
    "api_covid_historico",
]

# DDL de cada tabela (mesma definição de database.py)
DDL = {
    "gold_diagnostico": """
        CREATE TABLE IF NOT EXISTS gold_diagnostico (
            id SERIAL PRIMARY KEY,
            diagnostico VARCHAR(100),
            total_pacientes INTEGER,
            media_freq_cardiaca FLOAT,
            media_sistolica FLOAT,
            media_diastolica FLOAT,
            media_temperatura FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """,
    "gold_estado": """
        CREATE TABLE IF NOT EXISTS gold_estado (
            id SERIAL PRIMARY KEY,
            estado VARCHAR(5),
            total_pacientes INTEGER,
            media_freq_cardiaca FLOAT,
            pacientes_pressao_alta INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """,
    "api_estados_ibge": """
        CREATE TABLE IF NOT EXISTS api_estados_ibge (
            id SERIAL PRIMARY KEY,
            estado_id INTEGER,
            sigla VARCHAR(5),
            nome_estado VARCHAR(100),
            regiao VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """,
    "api_covid_brasil": """
        CREATE TABLE IF NOT EXISTS api_covid_brasil (
            id SERIAL PRIMARY KEY,
            pais VARCHAR(50),
            casos_total BIGINT,
            mortes_total BIGINT,
            recuperados BIGINT,
            ativos BIGINT,
            casos_por_milhao FLOAT,
            mortes_por_milhao FLOAT,
            testes_total BIGINT,
            populacao BIGINT,
            data_atualizacao TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """,
    "api_covid_historico": """
        CREATE TABLE IF NOT EXISTS api_covid_historico (
            id SERIAL PRIMARY KEY,
            data VARCHAR(20),
            casos_acumulados BIGINT,
            mortes_acumuladas BIGINT,
            recuperados_acumulados BIGINT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """,
}


def sync_table(local_conn, rds_conn, table: str):
    """Copia dados de uma tabela local para o RDS."""
    lcur = local_conn.cursor()
    rcur = rds_conn.cursor()

    # Cria tabela no RDS se não existir
    rcur.execute(DDL[table])
    rds_conn.commit()

    # Limpa dados antigos no RDS para evitar duplicatas
    rcur.execute(f"DELETE FROM {table};")
    rds_conn.commit()

    # Lê dados do local (exclui coluna id e created_at)
    lcur.execute(f"SELECT * FROM {table};")
    rows = lcur.fetchall()
    if not rows:
        print(f"  {table}: vazia localmente, pulando.")
        return

    # Pega nomes de coluna (sem id e created_at)
    col_names = [desc[0] for desc in lcur.description]
    skip = {"id", "created_at"}
    keep_idx = [i for i, c in enumerate(col_names) if c not in skip]
    keep_cols = [col_names[i] for i in keep_idx]

    placeholders = ", ".join(["%s"] * len(keep_cols))
    cols = ", ".join(keep_cols)
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    count = 0
    for row in rows:
        values = [row[i] for i in keep_idx]
        rcur.execute(sql, values)
        count += 1

    rds_conn.commit()
    print(f"  {table}: {count} registros sincronizados.")

    lcur.close()
    rcur.close()


def main():
    if not RDS["password"]:
        print("ERRO: RDS_PASSWORD não definida.")
        print("  export RDS_PASSWORD='<senha do RDS>'")
        sys.exit(1)

    print("Conectando ao PostgreSQL local...")
    local_conn = psycopg2.connect(**LOCAL)

    print(f"Conectando ao RDS: {RDS['host']}...")
    rds_conn = psycopg2.connect(**RDS)

    print("Sincronizando tabelas:")
    for table in TABLES:
        try:
            sync_table(local_conn, rds_conn, table)
        except Exception as e:
            print(f"  {table}: ERRO — {e}")

    local_conn.close()
    rds_conn.close()
    print("Sincronização concluída!")


if __name__ == "__main__":
    main()
