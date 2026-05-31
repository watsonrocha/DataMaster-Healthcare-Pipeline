"""
Métricas reais de qualidade, custo e performance do pipeline.

Implementa:
  - Data Quality Score (DQS) composicional
  - Métricas de completude, unicidade, consistência, atualidade
  - Estimativa de custo por execução (compute + storage + transfer)
  - Performance profiling por estágio
  - SLA tracking
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    """Relatório de qualidade de dados."""

    layer: str
    timestamp: str
    total_records: int
    total_columns: int
    completeness: float
    uniqueness: float
    consistency: float
    timeliness: float
    dqs_score: float
    column_details: dict = field(default_factory=dict)
    anomalies: list = field(default_factory=list)


@dataclass
class CostReport:
    """Relatório de custos estimados."""

    execution_id: str
    timestamp: str
    compute_cost_usd: float
    storage_cost_usd: float
    transfer_cost_usd: float
    total_cost_usd: float
    records_processed: int
    cost_per_record_usd: float
    breakdown: dict = field(default_factory=dict)


@dataclass
class PerformanceReport:
    """Relatório de performance."""

    execution_id: str
    timestamp: str
    total_duration_seconds: float
    records_per_second: float
    stages: dict = field(default_factory=dict)
    bottleneck_stage: str = ""
    memory_peak_mb: float = 0.0
    shuffle_bytes: int = 0


class DataQualityAnalyzer:
    """Analisa qualidade de dados com métricas composicionais."""

    WEIGHTS = {
        "completeness": 0.30,
        "uniqueness": 0.25,
        "consistency": 0.25,
        "timeliness": 0.20,
    }

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def analyze(
        self,
        df: DataFrame,
        layer: str,
        key_columns: Optional[list] = None,
        timestamp_col: str = "timestamp",
        max_age_hours: int = 24,
    ) -> QualityReport:
        """Executa análise completa de qualidade."""
        total = df.count()
        if total == 0:
            return QualityReport(
                layer=layer,
                timestamp=datetime.now().isoformat(),
                total_records=0,
                total_columns=len(df.columns),
                completeness=0.0,
                uniqueness=0.0,
                consistency=0.0,
                timeliness=0.0,
                dqs_score=0.0,
            )

        completeness = self._measure_completeness(df, total)
        uniqueness = self._measure_uniqueness(df, total, key_columns)
        consistency = self._measure_consistency(df, total)
        timeliness = self._measure_timeliness(df, timestamp_col, max_age_hours)
        column_details = self._column_profiling(df, total)
        anomalies = self._detect_anomalies(df, column_details)

        dqs = (
            self.WEIGHTS["completeness"] * completeness
            + self.WEIGHTS["uniqueness"] * uniqueness
            + self.WEIGHTS["consistency"] * consistency
            + self.WEIGHTS["timeliness"] * timeliness
        )

        report = QualityReport(
            layer=layer,
            timestamp=datetime.now().isoformat(),
            total_records=total,
            total_columns=len(df.columns),
            completeness=round(completeness, 4),
            uniqueness=round(uniqueness, 4),
            consistency=round(consistency, 4),
            timeliness=round(timeliness, 4),
            dqs_score=round(dqs, 4),
            column_details=column_details,
            anomalies=anomalies,
        )

        self._log_report(report)
        return report

    def _measure_completeness(self, df: DataFrame, total: int) -> float:
        """Mede a proporção de valores não-nulos."""
        null_counts = df.select([F.sum(F.when(F.isnull(c), 1).otherwise(0)).alias(c) for c in df.columns]).collect()[0]

        total_cells = total * len(df.columns)
        total_nulls = sum(null_counts[c] or 0 for c in df.columns)
        return 1.0 - (total_nulls / total_cells) if total_cells > 0 else 0.0

    def _measure_uniqueness(self, df: DataFrame, total: int, key_columns: Optional[list]) -> float:
        """Mede a proporção de registros únicos."""
        if key_columns:
            distinct_count = df.select(key_columns).distinct().count()
        else:
            distinct_count = df.distinct().count()
        return distinct_count / total if total > 0 else 0.0

    def _measure_consistency(self, df: DataFrame, total: int) -> float:
        """Mede consistência de dados (validações de domínio)."""
        checks = []

        if "heart_rate" in df.columns:
            valid_hr = df.filter((F.col("heart_rate") >= 30) & (F.col("heart_rate") <= 220)).count()
            checks.append(valid_hr / total)

        if "temperatura" in df.columns:
            valid_temp = df.filter((F.col("temperatura") >= 34.0) & (F.col("temperatura") <= 42.0)).count()
            checks.append(valid_temp / total)

        if "saturacao_o2" in df.columns:
            valid_o2 = df.filter((F.col("saturacao_o2") >= 70) & (F.col("saturacao_o2") <= 100)).count()
            checks.append(valid_o2 / total)

        if "idade" in df.columns:
            valid_age = df.filter((F.col("idade") >= 0) & (F.col("idade") <= 130)).count()
            checks.append(valid_age / total)

        if "sexo" in df.columns:
            valid_sex = df.filter(F.col("sexo").isin("M", "F")).count()
            checks.append(valid_sex / total)

        return sum(checks) / len(checks) if checks else 1.0

    def _measure_timeliness(self, df: DataFrame, timestamp_col: str, max_age_hours: int) -> float:
        """Mede atualidade dos dados."""
        if timestamp_col not in df.columns:
            return 1.0

        try:
            latest = df.agg(F.max(F.col(timestamp_col))).collect()[0][0]
            if latest is None:
                return 0.0

            if isinstance(latest, str):
                from datetime import datetime as dt

                latest = dt.strptime(latest, "%Y-%m-%d %H:%M:%S")

            age_hours = (datetime.now() - latest).total_seconds() / 3600
            return max(0.0, 1.0 - (age_hours / max_age_hours))
        except Exception:
            return 0.5

    def _column_profiling(self, df: DataFrame, total: int) -> dict:
        """Profiling detalhado por coluna."""
        profiles = {}
        for col_name in df.columns:
            null_count = df.filter(F.isnull(col_name)).count()
            distinct_count = df.select(col_name).distinct().count()

            profile = {
                "null_count": null_count,
                "null_pct": round(null_count / total * 100, 2) if total > 0 else 0,
                "distinct_count": distinct_count,
                "distinct_pct": round(distinct_count / total * 100, 2) if total > 0 else 0,
            }

            col_type = str(df.schema[col_name].dataType)
            if "Integer" in col_type or "Float" in col_type or "Double" in col_type:
                stats = df.select(
                    F.min(col_name).alias("min"),
                    F.max(col_name).alias("max"),
                    F.mean(col_name).alias("mean"),
                    F.stddev(col_name).alias("stddev"),
                ).collect()[0]
                profile.update(
                    {
                        "min": stats["min"],
                        "max": stats["max"],
                        "mean": round(stats["mean"], 4) if stats["mean"] else None,
                        "stddev": round(stats["stddev"], 4) if stats["stddev"] else None,
                    }
                )

            profiles[col_name] = profile

        return profiles

    def _detect_anomalies(self, df: DataFrame, column_details: dict) -> list:
        """Detecta anomalias nos dados."""
        anomalies = []

        for col_name, profile in column_details.items():
            if profile["null_pct"] > 30:
                anomalies.append(
                    {
                        "type": "high_null_ratio",
                        "column": col_name,
                        "value": profile["null_pct"],
                        "threshold": 30,
                        "severity": "warning",
                    }
                )

            if profile["distinct_count"] == 1 and profile["null_count"] == 0:
                anomalies.append(
                    {
                        "type": "constant_column",
                        "column": col_name,
                        "severity": "info",
                    }
                )

            if "stddev" in profile and profile["stddev"] is not None:
                if profile["stddev"] == 0 and profile["distinct_count"] > 1:
                    anomalies.append(
                        {
                            "type": "zero_variance",
                            "column": col_name,
                            "severity": "warning",
                        }
                    )

        return anomalies

    def _log_report(self, report: QualityReport) -> None:
        logger.info("=== Data Quality Report [%s] ===", report.layer)
        logger.info("  DQS Score: %.2f%%", report.dqs_score * 100)
        logger.info("  Completeness: %.2f%%", report.completeness * 100)
        logger.info("  Uniqueness: %.2f%%", report.uniqueness * 100)
        logger.info("  Consistency: %.2f%%", report.consistency * 100)
        logger.info("  Timeliness: %.2f%%", report.timeliness * 100)
        logger.info("  Records: %d | Columns: %d", report.total_records, report.total_columns)
        if report.anomalies:
            logger.warning("  Anomalies detected: %d", len(report.anomalies))
            for a in report.anomalies:
                logger.warning("    - %s: %s (%s)", a["type"], a.get("column", ""), a["severity"])


class CostEstimator:
    """Estima custos de execução do pipeline na AWS."""

    # Preços AWS us-east-1 (referência)
    PRICING = {
        "emr_m5_xlarge_per_hour": 0.126,
        "s3_storage_per_gb_month": 0.023,
        "s3_put_per_1000": 0.005,
        "s3_get_per_1000": 0.0004,
        "data_transfer_per_gb": 0.09,
        "rds_t3_medium_per_hour": 0.068,
        "msk_m5_large_per_hour": 0.21,
    }

    def __init__(self):
        self._start_time = None
        self._records_processed = 0
        self._storage_bytes = 0
        self._transfer_bytes = 0

    def start_tracking(self) -> None:
        self._start_time = time.time()
        self._records_processed = 0
        self._storage_bytes = 0
        self._transfer_bytes = 0

    def add_records(self, count: int) -> None:
        self._records_processed += count

    def add_storage(self, bytes_written: int) -> None:
        self._storage_bytes += bytes_written

    def add_transfer(self, bytes_transferred: int) -> None:
        self._transfer_bytes += bytes_transferred

    def estimate(
        self,
        emr_nodes: int = 2,
        instance_type: str = "m5.xlarge",
        include_kafka: bool = True,
        include_rds: bool = True,
    ) -> CostReport:
        """Gera relatório de custos estimados."""
        duration_hours = (time.time() - self._start_time) / 3600 if self._start_time else 0

        compute = emr_nodes * self.PRICING["emr_m5_xlarge_per_hour"] * duration_hours
        storage_gb = self._storage_bytes / (1024**3)
        storage = storage_gb * self.PRICING["s3_storage_per_gb_month"] / 30
        transfer_gb = self._transfer_bytes / (1024**3)
        transfer = transfer_gb * self.PRICING["data_transfer_per_gb"]

        breakdown = {
            "emr_compute": round(compute, 4),
            "s3_storage": round(storage, 4),
            "s3_requests": round((self._records_processed / 1000) * self.PRICING["s3_put_per_1000"], 4),
            "data_transfer": round(transfer, 4),
        }

        if include_kafka:
            kafka_cost = self.PRICING["msk_m5_large_per_hour"] * duration_hours * 3
            breakdown["kafka_msk"] = round(kafka_cost, 4)

        if include_rds:
            rds_cost = self.PRICING["rds_t3_medium_per_hour"] * duration_hours
            breakdown["rds_postgres"] = round(rds_cost, 4)

        total = sum(breakdown.values())

        return CostReport(
            execution_id=f"exec-{int(time.time())}",
            timestamp=datetime.now().isoformat(),
            compute_cost_usd=round(compute, 4),
            storage_cost_usd=round(storage, 4),
            transfer_cost_usd=round(transfer, 4),
            total_cost_usd=round(total, 4),
            records_processed=self._records_processed,
            cost_per_record_usd=round(total / max(self._records_processed, 1), 8),
            breakdown=breakdown,
        )


class PerformanceProfiler:
    """Profiler de performance do pipeline."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._stages: dict = {}
        self._start_time: Optional[float] = None
        self._total_records = 0

    def start(self) -> None:
        self._start_time = time.time()
        self._stages = {}

    def start_stage(self, name: str) -> None:
        self._stages[name] = {"start": time.time(), "end": None, "records": 0}

    def end_stage(self, name: str, records: int = 0) -> None:
        if name in self._stages:
            self._stages[name]["end"] = time.time()
            self._stages[name]["records"] = records
            self._stages[name]["duration"] = self._stages[name]["end"] - self._stages[name]["start"]
            self._total_records += records

    def report(self) -> PerformanceReport:
        """Gera relatório de performance."""
        total_duration = time.time() - self._start_time if self._start_time else 0

        stage_metrics = {}
        bottleneck = ""
        max_duration = 0

        for name, data in self._stages.items():
            duration = data.get("duration", 0)
            records = data.get("records", 0)
            stage_metrics[name] = {
                "duration_seconds": round(duration, 3),
                "records": records,
                "records_per_second": round(records / max(duration, 0.001), 2),
                "pct_of_total": round(duration / max(total_duration, 0.001) * 100, 1),
            }
            if duration > max_duration:
                max_duration = duration
                bottleneck = name

        return PerformanceReport(
            execution_id=f"perf-{int(time.time())}",
            timestamp=datetime.now().isoformat(),
            total_duration_seconds=round(total_duration, 3),
            records_per_second=round(self._total_records / max(total_duration, 0.001), 2),
            stages=stage_metrics,
            bottleneck_stage=bottleneck,
        )

    def log_spark_metrics(self) -> dict:
        """Extrai métricas do Spark para o relatório."""
        sc = self.spark.sparkContext
        status = sc.statusTracker()

        active_jobs = status.getActiveJobIds()
        active_stages = status.getActiveStageIds()

        return {
            "active_jobs": len(active_jobs),
            "active_stages": len(active_stages),
            "default_parallelism": sc.defaultParallelism,
            "app_name": sc.appName,
        }
