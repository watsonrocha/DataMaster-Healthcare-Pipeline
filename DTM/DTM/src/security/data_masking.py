"""
Mascaramento de dados sensíveis para conformidade com a LGPD.

Técnicas implementadas:
  • CPF: exibição parcial (***123***)
  • patient_id / medical_record_number: hash SHA-256 com chave
  • nome: pseudonimização (iniciais + hash curto)
  • email: domínio preservado, local mascarado
  • telefone: últimos 4 dígitos preservados
  • genérico: prefixo de 3 caracteres + ***
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataMasker:
    def __init__(self, encryption_key: str = None):
        from config.settings import Config

        self.encryption_key = encryption_key or Config.ENCRYPTION_KEY

    def mask_data(self, df: DataFrame, column: str) -> DataFrame:
        """Aplica a técnica de mascaramento adequada para o campo."""
        if column not in df.columns:
            return df

        if column == "cpf":
            return self._mask_cpf(df, column)
        elif column in {"patient_id", "medical_record_number"}:
            return self._hash_field(df, column)
        elif column == "nome":
            return self._mask_nome(df, column)
        elif column == "email":
            return self._mask_email(df, column)
        elif column == "telefone":
            return self._mask_telefone(df, column)
        else:
            return self._mask_generic(df, column)

    def _mask_cpf(self, df: DataFrame, col_name: str) -> DataFrame:
        """CPF: ***123*** — exibe apenas os 3 dígitos centrais."""
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isNotNull(),
                F.concat(F.lit("***"), F.substring(col_name, 5, 3), F.lit("***")),
            ).otherwise(F.lit(None)),
        )

    def _hash_field(self, df: DataFrame, col_name: str) -> DataFrame:
        """Hash SHA-256 com chave de criptografia."""
        return df.withColumn(
            col_name,
            F.sha2(F.concat(F.col(col_name), F.lit(self.encryption_key)), 256),
        )

    def _mask_nome(self, df: DataFrame, col_name: str) -> DataFrame:
        """Nome: primeira letra + hash curto (pseudonimização)."""
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isNotNull(),
                F.concat(
                    F.substring(col_name, 1, 1),
                    F.lit("***_"),
                    F.substring(F.sha2(F.col(col_name), 256), 1, 6),
                ),
            ).otherwise(F.lit(None)),
        )

    def _mask_email(self, df: DataFrame, col_name: str) -> DataFrame:
        """Email: m***@dominio.com — preserva domínio."""
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isNotNull(),
                F.concat(
                    F.substring(col_name, 1, 1),
                    F.lit("***@"),
                    F.element_at(F.split(col_name, "@"), 2),
                ),
            ).otherwise(F.lit(None)),
        )

    def _mask_telefone(self, df: DataFrame, col_name: str) -> DataFrame:
        """Telefone: (XX) XXXXX-1234 — preserva últimos 4 dígitos."""
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isNotNull(),
                F.concat(
                    F.lit("(**) *****-"),
                    F.substring(col_name, -4, 4),
                ),
            ).otherwise(F.lit(None)),
        )

    def _mask_generic(self, df: DataFrame, col_name: str) -> DataFrame:
        """Genérico: preserva 3 primeiros caracteres + ***."""
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isNotNull(),
                F.regexp_replace(col_name, r"(^.{3}).*", r"$1***"),
            ).otherwise(F.lit(None)),
        )
