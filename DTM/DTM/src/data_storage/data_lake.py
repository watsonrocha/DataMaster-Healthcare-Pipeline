"""
Gerenciador do Data Lake com arquitetura em camadas (Medallion).

Camadas:
  • Bronze: dados brutos, sem transformação
  • Silver: dados limpos, validados e enriquecidos
  • Gold:   dados agregados, prontos para análise/BI
"""

import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class DataLakeManager:
    def __init__(self, spark: SparkSession, storage_config: dict = None):
        from config.settings import Config

        self.spark = spark
        self.config = storage_config or Config.STORAGE

    # ── Escrita genérica ─────────────────────────────────────────────────
    def _save(self, df: DataFrame, layer: str, path: str, partition_cols: list = None, mode: str = "overwrite") -> str:
        full_path = f"{self.config[layer]}/{path}"
        writer = df.write.mode(mode).format("parquet")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(full_path)
        count = df.count()
        logger.info("Salvos %d registros em %s (%s)", count, full_path, layer.upper())
        return full_path

    # ── Camada Bronze (dados brutos) ─────────────────────────────────────
    def save_bronze(self, df: DataFrame, path: str, partition_cols: list = None) -> str:
        return self._save(df, "bronze", path, partition_cols)

    # ── Camada Silver (dados limpos) ─────────────────────────────────────
    def save_silver(self, df: DataFrame, path: str, partition_cols: list = None) -> str:
        return self._save(df, "silver", path, partition_cols)

    # ── Camada Gold (dados agregados) ────────────────────────────────────
    def save_gold(self, df: DataFrame, path: str, partition_cols: list = None) -> str:
        return self._save(df, "gold", path, partition_cols)

    # ── Leitura ──────────────────────────────────────────────────────────
    def read_layer(self, layer: str, path: str) -> DataFrame:
        full_path = f"{self.config[layer]}/{path}"
        logger.info("Lendo dados de %s", full_path)
        return self.spark.read.parquet(full_path)

    # ── Streaming para Data Lake ─────────────────────────────────────────
    def stream_to_lake(self, stream_df: DataFrame, path: str, partition_cols: list = None):
        full_path = f"{self.config['silver']}/{path}"
        writer = (
            stream_df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", full_path)
            .option("checkpointLocation", self.config["checkpoints"])
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        return writer.start()
