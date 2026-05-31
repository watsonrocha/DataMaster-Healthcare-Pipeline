"""
Observabilidade e monitoramento do pipeline de dados.

Funcionalidades:
  • Métricas de latência (processing_lag_ms) e tamanho de registros
  • Verificação de qualidade de dados (valores nulos, estatísticas)
  • Logging estruturado com níveis configuráveis
  • Alertas para anomalias detectadas
"""

import logging
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class PipelineMetrics:
    def __init__(self, spark: SparkSession, exporter=None):
        self.spark = spark
        self._timers: dict = {}
        # Exportador Prometheus opcional. Quando presente, as métricas
        # coletadas aqui também são publicadas em formato Prometheus.
        self.exporter = exporter

    # ── Métricas em colunas ──────────────────────────────────────────────
    def add_metrics(self, df: DataFrame) -> DataFrame:
        """Adiciona colunas de métrica ao DataFrame."""
        df = df.withColumn(
            "processing_lag_ms",
            (F.current_timestamp().cast("long") - F.col("timestamp").cast("long")) * 1000,
        )
        df = df.withColumn(
            "record_size_bytes",
            F.length(F.to_json(F.struct([F.col(c) for c in df.columns]))),
        )
        return df

    # ── Qualidade de dados ───────────────────────────────────────────────
    def check_data_quality(self, df: DataFrame, stage: str = "") -> dict:
        """Verifica qualidade dos dados e retorna relatório."""
        total = df.count()
        null_counts = {}
        null_ratios = {}
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_ratios[col_name] = (null_count / total) if total > 0 else 0.0
            if null_count > 0:
                null_counts[col_name] = null_count

        null_pct = {k: round(v / total * 100, 2) for k, v in null_counts.items()} if total > 0 else {}
        duplicates = total - df.dropDuplicates().count()

        report = {
            "stage": stage,
            "total_records": total,
            "columns": len(df.columns),
            "null_columns": null_counts,
            "null_pct": null_pct,
            "duplicates": duplicates,
        }

        # Publica no Prometheus (se exportador configurado)
        if self.exporter is not None and total > 0:
            try:
                self.exporter.record_processed(stage=stage, source="batch", count=total)
                self.exporter.set_quality_metrics(
                    layer=stage,
                    null_ratios=null_ratios,
                    duplicate_ratio=duplicates / total,
                )
            except Exception as exc:  # noqa: BLE001 - métricas não devem quebrar o pipeline
                logger.debug("Falha ao exportar métricas de qualidade: %s", exc)

        logger.info("=== Qualidade de Dados [%s] ===", stage)
        logger.info("  Total registros: %d", total)
        logger.info("  Colunas: %d", len(df.columns))
        if null_counts:
            logger.info("  Campos com nulos: %s", null_pct)
        logger.info("  Duplicatas: %d", report["duplicates"])

        return report

    # ── Timer de etapas ──────────────────────────────────────────────────
    @contextmanager
    def timer(self, stage_name: str):
        """Context manager para medir tempo de execução de uma etapa."""
        start = time.time()
        logger.info("▶ Iniciando: %s", stage_name)
        try:
            yield
        finally:
            elapsed = time.time() - start
            self._timers[stage_name] = elapsed
            if self.exporter is not None:
                try:
                    self.exporter.stage_duration.labels(stage=stage_name).observe(elapsed)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Falha ao exportar duração da etapa: %s", exc)
            logger.info("✓ %s concluído em %.2fs", stage_name, elapsed)

    def get_timing_report(self) -> dict:
        """Retorna tempos de execução de todas as etapas."""
        return dict(self._timers)

    # ── Alertas ──────────────────────────────────────────────────────────
    def alert(self, message: str, severity: str = "WARNING") -> None:
        log_fn = getattr(logger, severity.lower(), logger.warning)
        log_fn("[ALERTA] %s", message)

    def check_thresholds(self, df: DataFrame) -> None:
        """Verifica limites e dispara alertas se necessário."""
        total = df.count()
        if total == 0:
            self.alert("DataFrame vazio — verifique a fonte de dados", "ERROR")
            return

        null_ratio = df.select([F.sum(F.when(F.isnull(c), 1).otherwise(0)).alias(c) for c in df.columns]).collect()[0]

        for col_name in df.columns:
            pct = (null_ratio[col_name] or 0) / total * 100
            if pct > 30:
                self.alert(f"Coluna '{col_name}' tem {pct:.1f}% de valores nulos")
