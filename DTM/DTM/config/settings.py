"""
Configurações centralizadas do pipeline de dados de saúde.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"


class Config:
    # ── Spark ────────────────────────────────────────────────────────────
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[1]")
    SPARK_APP_NAME = "HealthcareDataPipeline"
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")

    # ── Fontes de dados ─────────────────────────────────────────────────
    DATA_SOURCES = {
        "government_api": os.getenv("GOV_API_URL", "https://dummyjson.com/users?limit=10"),
        "kafka_brokers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
        "jdbc_url": os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/healthcare"),
    }

    # ── Armazenamento (Data Lake – camadas Bronze / Silver / Gold) ──────
    DATA_LAKE_ROOT = os.getenv("DATA_LAKE_PATH", str(OUTPUT_DIR / "data_lake"))
    STORAGE = {
        "bronze": os.path.join(DATA_LAKE_ROOT, "bronze"),
        "silver": os.path.join(DATA_LAKE_ROOT, "silver"),
        "gold": os.path.join(DATA_LAKE_ROOT, "gold"),
        "checkpoints": os.path.join(DATA_LAKE_ROOT, "checkpoints"),
    }

    # ── Segurança / LGPD ───────────────────────────────────────────────
    ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "chave-segura-exemplo-2025")
    SENSITIVE_FIELDS = {"cpf", "patient_id", "nome", "email", "telefone"}

    # ── Geração de dados simulados ──────────────────────────────────────
    SIMULATED_RECORDS = int(os.getenv("SIMULATED_RECORDS", "100"))

    # ── Logging ─────────────────────────────────────────────────────────
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = str(OUTPUT_DIR / "pipeline.log")

    # ── Observabilidade (Prometheus) ────────────────────────────────────
    METRICS_ENABLED = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
    # Mantém o servidor de métricas vivo após o pipeline terminar para que
    # o Prometheus consiga coletar (scrape) as métricas. Útil em container.
    METRICS_KEEP_ALIVE = os.getenv("METRICS_KEEP_ALIVE", "false").lower() == "true"
    # Se > 0, reexecuta o pipeline a cada N segundos (alimenta gráficos de taxa).
    METRICS_LOOP_INTERVAL = int(os.getenv("METRICS_LOOP_INTERVAL", "0"))
