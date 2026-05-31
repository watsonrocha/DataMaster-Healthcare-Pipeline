"""
Armazenamento em PostgreSQL.

Salva os dados processados (Gold) em tabelas do PostgreSQL,
demonstrando integração com banco de dados relacional.
"""

import logging
import os

logger = logging.getLogger(__name__)

# Configuração padrão do PostgreSQL (via docker-compose)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "healthcare"),
    "user": os.getenv("DB_USER", "pipeline"),
    "password": os.getenv("DB_PASSWORD", "pipeline123"),
}


def _get_connection():
    """Cria conexão com PostgreSQL."""
    import psycopg2

    return psycopg2.connect(**DB_CONFIG)


def init_database():
    """Cria as tabelas no PostgreSQL (se não existirem)."""
    logger.info("Inicializando banco de dados PostgreSQL...")
    conn = _get_connection()
    cur = conn.cursor()

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_estado (
            id SERIAL PRIMARY KEY,
            estado VARCHAR(5),
            total_pacientes INTEGER,
            media_freq_cardiaca FLOAT,
            pacientes_pressao_alta INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_estados_ibge (
            id SERIAL PRIMARY KEY,
            estado_id INTEGER,
            sigla VARCHAR(5),
            nome_estado VARCHAR(100),
            regiao VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_covid_historico (
            id SERIAL PRIMARY KEY,
            data VARCHAR(20),
            casos_acumulados BIGINT,
            mortes_acumuladas BIGINT,
            recuperados_acumulados BIGINT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Tabelas criadas com sucesso!")


def save_dataframe_to_postgres(df, table_name: str):
    """Salva um DataFrame PySpark em uma tabela PostgreSQL."""
    rows = df.collect()
    if not rows:
        logger.warning("DataFrame vazio, nada a salvar em %s", table_name)
        return

    columns = df.columns
    conn = _get_connection()
    cur = conn.cursor()

    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    count = 0
    for row in rows:
        values = [row[c] for c in columns]
        cur.execute(sql, values)
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Salvos %d registros na tabela '%s' (PostgreSQL)", count, table_name)


def save_api_data_to_postgres(api_data: dict[str, list[dict]]):
    """Salva dados das APIs no PostgreSQL."""
    conn = _get_connection()
    cur = conn.cursor()

    # Estados IBGE
    for rec in api_data.get("estados_ibge", []):
        cur.execute(
            "INSERT INTO api_estados_ibge (estado_id, sigla, nome_estado, regiao) VALUES (%s, %s, %s, %s)",
            (rec["estado_id"], rec["sigla"], rec["nome_estado"], rec["regiao"]),
        )

    # COVID Brasil
    for rec in api_data.get("covid_brasil", []):
        cur.execute(
            "INSERT INTO api_covid_brasil "
            "(pais, casos_total, mortes_total, recuperados, ativos, "
            "casos_por_milhao, mortes_por_milhao, testes_total, populacao, data_atualizacao) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                rec["pais"],
                rec["casos_total"],
                rec["mortes_total"],
                rec["recuperados"],
                rec["ativos"],
                rec["casos_por_milhao"],
                rec["mortes_por_milhao"],
                rec["testes_total"],
                rec["populacao"],
                rec["data_atualizacao"],
            ),
        )

    # Histórico COVID
    for rec in api_data.get("covid_historico", []):
        cur.execute(
            "INSERT INTO api_covid_historico "
            "(data, casos_acumulados, mortes_acumuladas, recuperados_acumulados) "
            "VALUES (%s, %s, %s, %s)",
            (rec["data"], rec["casos_acumulados"], rec["mortes_acumuladas"], rec["recuperados_acumulados"]),
        )

    conn.commit()
    cur.close()
    conn.close()

    total = (
        len(api_data.get("estados_ibge", []))
        + len(api_data.get("covid_brasil", []))
        + len(api_data.get("covid_historico", []))
    )
    logger.info("API data: %d registros salvos no PostgreSQL", total)
