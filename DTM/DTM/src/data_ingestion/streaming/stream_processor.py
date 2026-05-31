"""
Processador de streaming (simulado e Kafka).

Para demonstração, inclui um pipeline fake que simula a ingestão
em tempo real sem necessidade de infraestrutura Kafka real.
"""

import logging
import random
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)

logger = logging.getLogger(__name__)


class StreamProcessor:
    SCHEMA = StructType(
        [
            StructField("patient_id", StringType()),
            StructField("cpf", StringType()),
            StructField("nome", StringType()),
            StructField("blood_pressure", StringType()),
            StructField("heart_rate", IntegerType()),
            StructField("temperatura", FloatType()),
            StructField("saturacao_o2", IntegerType()),
            StructField("timestamp", StringType()),
        ]
    )

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ── Kafka real (quando disponível) ───────────────────────────────────
    def from_kafka(self, topic: str, brokers: str = "localhost:9092") -> DataFrame:
        logger.info("Conectando ao Kafka topic=%s brokers=%s", topic, brokers)
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
            .selectExpr(
                "CAST(timestamp AS TIMESTAMP) as event_time",
                "CAST(value AS STRING) as json_data",
            )
            .select(
                "event_time",
                F.from_json("json_data", self.SCHEMA).alias("data"),
            )
            .select("event_time", "data.*")
        )

    # ── Simulação de streaming (sem Kafka) ──────────────────────────────
    def simulate_stream(self, n_events: int = 50) -> DataFrame:
        """Gera dados que simulam um micro-batch de streaming."""
        logger.info("Gerando %d eventos simulados de streaming", n_events)

        data = []
        for _ in range(n_events):
            sistolica = random.randint(80, 180)
            diastolica = random.randint(50, 120)
            data.append(
                (
                    f"PAC-{random.randint(100000, 999999)}",
                    f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}",
                    f"Paciente_Stream_{random.randint(1, 100)}",
                    f"{sistolica}/{diastolica}",
                    random.randint(50, 130),
                    round(random.uniform(35.5, 40.5), 1),
                    random.randint(88, 100),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
            )

        df = self.spark.createDataFrame(data, self.SCHEMA)
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        return df
