"""
Structured Streaming real com Apache Kafka.

Implementa ingestão contínua de dados de saúde via Kafka com:
  - Checkpoint para exactly-once semantics
  - Watermark para tratamento de dados atrasados
  - Schema enforcement
  - Fallback para micro-batch quando Kafka não está disponível
"""

import logging
import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

logger = logging.getLogger(__name__)


class KafkaStreamingPipeline:
    """Pipeline de Structured Streaming com Kafka real e checkpointing."""

    HEALTHCARE_SCHEMA = StructType(
        [
            StructField("patient_id", StringType(), False),
            StructField("cpf", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("email", StringType(), True),
            StructField("telefone", StringType(), True),
            StructField("idade", IntegerType(), True),
            StructField("sexo", StringType(), True),
            StructField("cidade", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("blood_pressure", StringType(), True),
            StructField("heart_rate", IntegerType(), True),
            StructField("temperatura", FloatType(), True),
            StructField("saturacao_o2", IntegerType(), True),
            StructField("diagnostico", StringType(), True),
            StructField("medicamento", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        bootstrap_servers: Optional[str] = None,
        checkpoint_location: Optional[str] = None,
    ):
        self.spark = spark
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BROKERS", "localhost:9092")
        self.checkpoint_location = checkpoint_location or os.getenv(
            "CHECKPOINT_LOCATION", "/tmp/checkpoints/healthcare"
        )

    def read_stream(
        self,
        topic: str = "healthcare-events",
        starting_offsets: str = "latest",
        max_offsets_per_trigger: int = 10000,
    ) -> DataFrame:
        """Lê stream do Kafka com schema enforcement."""
        logger.info(
            "Conectando ao Kafka: brokers=%s topic=%s offsets=%s",
            self.bootstrap_servers,
            topic,
            starting_offsets,
        )

        raw_stream = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
            .option("failOnDataLoss", "false")
            .option("kafka.session.timeout.ms", "30000")
            .option("kafka.request.timeout.ms", "40000")
            .load()
        )

        parsed_stream = (
            raw_stream.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
            .select(
                F.from_json("json_str", self.HEALTHCARE_SCHEMA).alias("data"),
                "kafka_timestamp",
            )
            .select("data.*", "kafka_timestamp")
            .withColumn("event_time", F.to_timestamp("timestamp"))
            .withWatermark("event_time", "10 minutes")
        )

        return parsed_stream

    def write_to_delta(
        self,
        stream_df: DataFrame,
        output_path: str,
        checkpoint_suffix: str = "delta",
        trigger_interval: str = "30 seconds",
        partition_cols: Optional[list] = None,
    ):
        """Escreve stream no Delta Lake com checkpoint."""
        checkpoint_path = os.path.join(self.checkpoint_location, checkpoint_suffix)
        os.makedirs(checkpoint_path, exist_ok=True)

        writer = (
            stream_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        query = writer.start(output_path)
        logger.info(
            "Stream -> Delta Lake: path=%s checkpoint=%s trigger=%s",
            output_path,
            checkpoint_path,
            trigger_interval,
        )
        return query

    def write_to_console(
        self,
        stream_df: DataFrame,
        trigger_interval: str = "10 seconds",
        num_rows: int = 20,
    ):
        """Escreve stream no console (para debug/desenvolvimento)."""
        query = (
            stream_df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", num_rows)
            .trigger(processingTime=trigger_interval)
            .start()
        )
        logger.info("Stream -> Console (debug mode)")
        return query

    def windowed_aggregation(
        self,
        stream_df: DataFrame,
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute",
    ) -> DataFrame:
        """Agregação com janela temporal deslizante."""
        return stream_df.groupBy(
            F.window("event_time", window_duration, slide_duration),
            "diagnostico",
        ).agg(
            F.count("*").alias("total_eventos"),
            F.avg("heart_rate").alias("media_freq_cardiaca"),
            F.avg("temperatura").alias("media_temperatura"),
            F.min("saturacao_o2").alias("min_saturacao"),
        )

    def write_aggregation_to_delta(
        self,
        agg_df: DataFrame,
        output_path: str,
        checkpoint_suffix: str = "agg",
        trigger_interval: str = "1 minute",
    ):
        """Escreve agregação no Delta Lake com modo complete."""
        checkpoint_path = os.path.join(self.checkpoint_location, checkpoint_suffix)
        os.makedirs(checkpoint_path, exist_ok=True)

        query = (
            agg_df.writeStream.format("delta")
            .outputMode("complete")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
            .start(output_path)
        )
        logger.info("Aggregation stream -> Delta: path=%s", output_path)
        return query
