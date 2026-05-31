"""
Exportador de métricas Prometheus para o pipeline de dados de saúde.

Expõe métricas em formato Prometheus via HTTP (:8000/metrics):
  - pipeline_records_processed_total
  - pipeline_stage_duration_seconds
  - pipeline_data_quality_null_ratio
  - pipeline_errors_total
  - pipeline_streaming_lag_seconds
  - pipeline_storage_bytes
"""

import logging
from typing import Optional

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    Info,
    start_http_server,
    CollectorRegistry,
    REGISTRY,
)

logger = logging.getLogger(__name__)


class PrometheusExporter:
    """Exporta métricas do pipeline para Prometheus."""

    def __init__(self, port: int = 8000, registry: Optional[CollectorRegistry] = None):
        self.port = port
        self.registry = registry or REGISTRY
        self._server_started = False

        # ── Counters ─────────────────────────────────────────────────
        self.records_processed = Counter(
            "pipeline_records_processed_total",
            "Total de registros processados pelo pipeline",
            ["stage", "source"],
            registry=self.registry,
        )

        self.errors_total = Counter(
            "pipeline_errors_total",
            "Total de erros no pipeline",
            ["stage", "error_type"],
            registry=self.registry,
        )

        self.api_requests = Counter(
            "pipeline_api_requests_total",
            "Requisições a APIs externas",
            ["api", "status"],
            registry=self.registry,
        )

        # ── Histograms ───────────────────────────────────────────────
        self.stage_duration = Histogram(
            "pipeline_stage_duration_seconds",
            "Duração de cada etapa do pipeline em segundos",
            ["stage"],
            buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600],
            registry=self.registry,
        )

        self.record_size = Histogram(
            "pipeline_record_size_bytes",
            "Tamanho dos registros em bytes",
            ["layer"],
            buckets=[100, 500, 1000, 5000, 10000, 50000],
            registry=self.registry,
        )

        # ── Gauges ───────────────────────────────────────────────────
        self.data_quality_null_ratio = Gauge(
            "pipeline_data_quality_null_ratio",
            "Proporção de valores nulos por coluna",
            ["column", "layer"],
            registry=self.registry,
        )

        self.data_quality_duplicate_ratio = Gauge(
            "pipeline_data_quality_duplicate_ratio",
            "Proporção de registros duplicados",
            ["layer"],
            registry=self.registry,
        )

        self.streaming_lag = Gauge(
            "pipeline_streaming_lag_seconds",
            "Lag do streaming em segundos",
            ["query"],
            registry=self.registry,
        )

        self.storage_size = Gauge(
            "pipeline_storage_bytes",
            "Tamanho do armazenamento por camada",
            ["layer", "format"],
            registry=self.registry,
        )

        self.active_records = Gauge(
            "pipeline_active_records",
            "Número de registros ativos por camada",
            ["layer"],
            registry=self.registry,
        )

        # ── Summaries ────────────────────────────────────────────────
        self.processing_latency = Summary(
            "pipeline_processing_latency_ms",
            "Latência de processamento em milissegundos",
            ["stage"],
            registry=self.registry,
        )

        # ── Info ─────────────────────────────────────────────────────
        self.pipeline_info = Info(
            "pipeline",
            "Informações do pipeline",
            registry=self.registry,
        )

    def start_server(self) -> None:
        """Inicia o servidor HTTP de métricas em background."""
        if self._server_started:
            return
        try:
            start_http_server(self.port, registry=self.registry)
            self._server_started = True
            logger.info("Prometheus metrics server started on :%d", self.port)
        except OSError as e:
            logger.warning("Could not start metrics server: %s", e)

    def track_stage(self, stage: str):
        """Context manager para rastrear duração de uma etapa."""
        return self.stage_duration.labels(stage=stage).time()

    def record_processed(self, stage: str, source: str, count: int = 1) -> None:
        self.records_processed.labels(stage=stage, source=source).inc(count)

    def record_error(self, stage: str, error_type: str) -> None:
        self.errors_total.labels(stage=stage, error_type=error_type).inc()

    def set_quality_metrics(self, layer: str, null_ratios: dict, duplicate_ratio: float) -> None:
        """Atualiza métricas de qualidade de dados."""
        for col, ratio in null_ratios.items():
            self.data_quality_null_ratio.labels(column=col, layer=layer).set(ratio)
        self.data_quality_duplicate_ratio.labels(layer=layer).set(duplicate_ratio)

    def set_storage_metrics(self, layer: str, size_bytes: int, fmt: str = "delta") -> None:
        self.storage_size.labels(layer=layer, format=fmt).set(size_bytes)

    def set_pipeline_info(self, version: str, environment: str) -> None:
        self.pipeline_info.info(
            {
                "version": version,
                "environment": environment,
                "framework": "pyspark",
            }
        )
