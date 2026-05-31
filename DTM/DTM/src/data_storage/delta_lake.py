"""
Gerenciador Delta Lake com suporte a operações avançadas.

Funcionalidades:
  - MERGE (upsert) para evitar duplicatas
  - Schema Evolution automática
  - Time Travel para auditoria e rollback
  - VACUUM para manutenção de storage
  - Otimização com Z-ORDER
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DeltaLakeManager:
    """Gerenciador do Data Lake usando Delta Lake."""

    def __init__(self, spark: SparkSession, storage_config: Optional[dict] = None):
        self.spark = spark
        if storage_config is None:
            from config.settings import Config

            storage_config = Config.STORAGE
        self.config = storage_config

        self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        self.spark.conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    # ══════════════════════════════════════════════════════════════════════
    # ESCRITA COM DELTA
    # ══════════════════════════════════════════════════════════════════════

    def save_delta(
        self,
        df: DataFrame,
        layer: str,
        path: str,
        partition_cols: Optional[list] = None,
        mode: str = "overwrite",
        merge_schema: bool = True,
    ) -> str:
        """Salva DataFrame em formato Delta com schema evolution."""
        full_path = f"{self.config[layer]}/{path}"
        writer = df.write.format("delta").mode(mode)

        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(full_path)
        count = df.count()
        logger.info("Delta save: %d records -> %s (%s)", count, full_path, layer)
        return full_path

    def save_bronze(self, df: DataFrame, path: str, partition_cols: Optional[list] = None) -> str:
        return self.save_delta(df, "bronze", path, partition_cols)

    def save_silver(self, df: DataFrame, path: str, partition_cols: Optional[list] = None) -> str:
        return self.save_delta(df, "silver", path, partition_cols)

    def save_gold(self, df: DataFrame, path: str, partition_cols: Optional[list] = None) -> str:
        return self.save_delta(df, "gold", path, partition_cols)

    # ══════════════════════════════════════════════════════════════════════
    # MERGE (UPSERT)
    # ══════════════════════════════════════════════════════════════════════

    def merge(
        self,
        source_df: DataFrame,
        target_path: str,
        merge_condition: str,
        layer: str = "silver",
        update_columns: Optional[list] = None,
    ) -> dict:
        """
        Executa MERGE (upsert) no Delta Lake.

        Quando o registro existe (match), atualiza as colunas especificadas.
        Quando não existe, insere o registro completo.

        Args:
            source_df: DataFrame com dados novos
            target_path: caminho relativo dentro da camada
            merge_condition: condição SQL de join (ex: "target.patient_id = source.patient_id")
            layer: camada do Data Lake
            update_columns: colunas a atualizar no match (None = todas)
        """
        from delta.tables import DeltaTable

        full_path = f"{self.config[layer]}/{target_path}"

        if not DeltaTable.isDeltaTable(self.spark, full_path):
            logger.info("Delta table not found, creating: %s", full_path)
            self.save_delta(source_df, layer, target_path, mode="overwrite")
            return {"inserted": source_df.count(), "updated": 0, "deleted": 0}

        target = DeltaTable.forPath(self.spark, full_path)

        if update_columns:
            update_set = {col: f"source.{col}" for col in update_columns}
        else:
            update_set = {col: f"source.{col}" for col in source_df.columns}

        merge_builder = (
            target.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsertAll()
        )

        merge_builder.execute()

        history = target.history(1).collect()[0]
        metrics = history["operationMetrics"]

        result = {
            "inserted": int(metrics.get("numTargetRowsInserted", 0)),
            "updated": int(metrics.get("numTargetRowsUpdated", 0)),
            "deleted": int(metrics.get("numTargetRowsDeleted", 0)),
        }

        logger.info(
            "MERGE completed: inserted=%d updated=%d deleted=%d",
            result["inserted"],
            result["updated"],
            result["deleted"],
        )
        return result

    def merge_scd_type2(
        self,
        source_df: DataFrame,
        target_path: str,
        key_columns: list,
        layer: str = "silver",
    ) -> dict:
        """
        Merge SCD Type 2 — mantém histórico de alterações.

        Registros alterados são "fechados" (is_current=false) e uma nova
        versão é inserida (is_current=true).
        """
        from delta.tables import DeltaTable

        full_path = f"{self.config[layer]}/{target_path}"

        source_with_meta = (
            source_df.withColumn("valid_from", F.current_timestamp())
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )

        if not DeltaTable.isDeltaTable(self.spark, full_path):
            self.save_delta(source_with_meta, layer, target_path, mode="overwrite")
            return {"inserted": source_df.count(), "closed": 0}

        target = DeltaTable.forPath(self.spark, full_path)
        key_condition = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)

        non_key_cols = [c for c in source_df.columns if c not in key_columns]
        change_condition = " OR ".join(f"target.{c} != source.{c}" for c in non_key_cols)

        (
            target.alias("target")
            .merge(source_with_meta.alias("source"), f"{key_condition} AND target.is_current = true")
            .whenMatchedUpdate(
                condition=change_condition,
                set={
                    "is_current": "false",
                    "valid_to": "source.valid_from",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        history = target.history(1).collect()[0]
        logger.info("SCD Type 2 merge completed: %s", history["operationMetrics"])
        return history["operationMetrics"]

    # ══════════════════════════════════════════════════════════════════════
    # TIME TRAVEL
    # ══════════════════════════════════════════════════════════════════════

    def read_version(self, layer: str, path: str, version: int) -> DataFrame:
        """Lê uma versão específica do Delta table (time travel)."""
        full_path = f"{self.config[layer]}/{path}"
        logger.info("Time travel: reading version %d from %s", version, full_path)
        return self.spark.read.format("delta").option("versionAsOf", version).load(full_path)

    def read_timestamp(self, layer: str, path: str, timestamp: str) -> DataFrame:
        """Lê dados em um timestamp específico (time travel)."""
        full_path = f"{self.config[layer]}/{path}"
        logger.info("Time travel: reading at %s from %s", timestamp, full_path)
        return self.spark.read.format("delta").option("timestampAsOf", timestamp).load(full_path)

    def read_layer(self, layer: str, path: str) -> DataFrame:
        """Lê a versão mais recente."""
        full_path = f"{self.config[layer]}/{path}"
        return self.spark.read.format("delta").load(full_path)

    # ══════════════════════════════════════════════════════════════════════
    # HISTÓRICO E AUDITORIA
    # ══════════════════════════════════════════════════════════════════════

    def history(self, layer: str, path: str, limit: int = 20) -> DataFrame:
        """Retorna o histórico de operações do Delta table."""
        from delta.tables import DeltaTable

        full_path = f"{self.config[layer]}/{path}"
        table = DeltaTable.forPath(self.spark, full_path)
        return table.history(limit)

    def describe_history(self, layer: str, path: str) -> list:
        """Retorna histórico legível para auditoria."""
        hist_df = self.history(layer, path)
        return [
            {
                "version": row["version"],
                "timestamp": str(row["timestamp"]),
                "operation": row["operation"],
                "user": row.get("userName", "N/A"),
                "metrics": row.get("operationMetrics", {}),
            }
            for row in hist_df.collect()
        ]

    # ══════════════════════════════════════════════════════════════════════
    # MANUTENÇÃO
    # ══════════════════════════════════════════════════════════════════════

    def vacuum(self, layer: str, path: str, retention_hours: int = 168) -> None:
        """Remove arquivos antigos para liberar espaço."""
        from delta.tables import DeltaTable

        full_path = f"{self.config[layer]}/{path}"
        table = DeltaTable.forPath(self.spark, full_path)

        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        table.vacuum(retention_hours)
        logger.info("Vacuum completed: %s (retention=%dh)", full_path, retention_hours)

    def optimize(self, layer: str, path: str, z_order_cols: Optional[list] = None) -> None:
        """Otimiza o layout dos arquivos Delta (compaction + Z-ORDER)."""
        full_path = f"{self.config[layer]}/{path}"

        if z_order_cols:
            cols = ", ".join(z_order_cols)
            self.spark.sql(f"OPTIMIZE delta.`{full_path}` ZORDER BY ({cols})")
            logger.info("Optimize + Z-ORDER (%s): %s", cols, full_path)
        else:
            self.spark.sql(f"OPTIMIZE delta.`{full_path}`")
            logger.info("Optimize (compaction): %s", full_path)

    # ══════════════════════════════════════════════════════════════════════
    # STREAMING PARA DELTA
    # ══════════════════════════════════════════════════════════════════════

    def stream_to_delta(
        self,
        stream_df: DataFrame,
        layer: str,
        path: str,
        partition_cols: Optional[list] = None,
        trigger_interval: str = "30 seconds",
    ):
        """Escreve stream no Delta Lake com checkpoint."""
        full_path = f"{self.config[layer]}/{path}"
        checkpoint_path = f"{self.config['checkpoints']}/{path}"

        writer = (
            stream_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(processingTime=trigger_interval)
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        query = writer.start(full_path)
        logger.info("Stream -> Delta: %s (checkpoint: %s)", full_path, checkpoint_path)
        return query
