"""
Extratores de dados em lote (batch) de múltiplas fontes.

Fontes suportadas:
  • Arquivos locais (CSV, JSON, Parquet)
  • APIs públicas (REST/HTTP)
  • Bancos de dados via JDBC
"""

import os
import logging
from typing import Dict, Optional

import requests
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class BatchDataExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ── Arquivos locais ──────────────────────────────────────────────────
    def from_csv(self, path: str, **kwargs) -> DataFrame:
        logger.info("Lendo CSV: %s", path)
        return self.spark.read.csv(path, header=True, inferSchema=True, **kwargs)

    def from_json(self, path: str) -> DataFrame:
        logger.info("Lendo JSON: %s", path)
        return self.spark.read.json(path)

    def from_parquet(self, path: str) -> DataFrame:
        logger.info("Lendo Parquet: %s", path)
        return self.spark.read.parquet(path)

    def from_file(self, path: str, file_type: str = "csv") -> DataFrame:
        loaders = {
            "csv": self.from_csv,
            "json": self.from_json,
            "parquet": self.from_parquet,
        }
        loader = loaders.get(file_type)
        if loader is None:
            raise ValueError(f"Tipo de arquivo não suportado: {file_type}")
        return loader(path)

    # ── API pública ──────────────────────────────────────────────────────
    def from_api(self, url: str, params: Optional[Dict] = None) -> DataFrame:
        logger.info("Consultando API: %s", url)
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        tmp = "/tmp/_api_response.json"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(response.text)
        return self.spark.read.json(tmp)

    # ── Banco de dados (JDBC) ────────────────────────────────────────────
    def from_database(self, query: str, jdbc_url: str = None, user: str = None, password: str = None) -> DataFrame:
        jdbc_url = jdbc_url or os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/healthcare")
        user = user or os.getenv("DB_USER", "postgres")
        password = password or os.getenv("DB_PASSWORD", "postgres")

        logger.info("Consultando banco via JDBC")
        return (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
