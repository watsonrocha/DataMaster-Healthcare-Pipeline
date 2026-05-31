"""
Transformações e limpeza de dados no pipeline batch.

Responsabilidades:
  • Remoção de duplicatas e valores nulos críticos
  • Enriquecimento: split de pressão arterial, categorização
  • Aplicação de mascaramento de campos sensíveis (LGPD)
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.security.data_masking import DataMasker

logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self, sensitive_fields: set = None):
        from config.settings import Config

        self.masker = DataMasker()
        self.sensitive_fields = sensitive_fields or Config.SENSITIVE_FIELDS

    # ── Limpeza básica ───────────────────────────────────────────────────
    def clean(self, df: DataFrame) -> DataFrame:
        """Remove duplicatas e registros sem patient_id ou timestamp."""
        before = df.count()
        df = df.dropDuplicates()
        df = df.na.drop(subset=["patient_id", "timestamp"])
        after = df.count()
        logger.info("Limpeza: %d → %d registros (removidos: %d)", before, after, before - after)
        return df.withColumn("processing_time", F.current_timestamp())

    # ── Enriquecimento de dados de saúde ─────────────────────────────────
    def enrich_healthcare(self, df: DataFrame) -> DataFrame:
        """Extrai sistólica/diastólica e categoriza pressão arterial."""
        if "blood_pressure" not in df.columns:
            return df

        bp_array = F.split(F.regexp_replace("blood_pressure", r"\s+", ""), "/")

        df = (
            df.withColumn("sistolica", bp_array.getItem(0).cast("integer"))
            .withColumn("diastolica", bp_array.getItem(1).cast("integer"))
            .withColumn(
                "categoria_pressao",
                F.when((F.col("sistolica") < 90) | (F.col("diastolica") < 60), "baixa")
                .when((F.col("sistolica") >= 140) | (F.col("diastolica") >= 90), "alta")
                .otherwise("normal"),
            )
        )

        if "heart_rate" in df.columns:
            df = df.withColumn(
                "categoria_fc",
                F.when(F.col("heart_rate") < 60, "bradicardia")
                .when(F.col("heart_rate") > 100, "taquicardia")
                .otherwise("normal"),
            )

        if "temperatura" in df.columns:
            df = df.withColumn(
                "febre",
                F.when(F.col("temperatura") >= 37.8, True).otherwise(False),
            )

        logger.info("Enriquecimento de dados de saúde aplicado")
        return df

    # ── Mascaramento LGPD ────────────────────────────────────────────────
    def apply_masking(self, df: DataFrame) -> DataFrame:
        """Aplica mascaramento nos campos sensíveis configurados."""
        for field in self.sensitive_fields:
            if field in df.columns:
                df = self.masker.mask_data(df, field)
                logger.info("Campo mascarado: %s", field)
        return df

    # ── Pipeline completo de transformação ───────────────────────────────
    def transform(self, df: DataFrame, mask: bool = True) -> DataFrame:
        """Executa limpeza → enriquecimento → mascaramento."""
        df = self.clean(df)
        df = self.enrich_healthcare(df)
        if mask:
            df = self.apply_masking(df)
        return df
