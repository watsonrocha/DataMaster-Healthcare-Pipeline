"""
Testes de integração PySpark para o pipeline de dados de saúde.

Testa o fluxo completo: extração -> ingestão -> transformação -> agregação
usando uma SparkSession local com Delta Lake.
"""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "DTM", "DTM"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    """Cria SparkSession para testes."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("healthcare-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_data(spark):
    """Gera DataFrame de amostra para testes."""
    data = [
        (
            "PAC-100001",
            "123.456.789-00",
            "Maria Silva",
            "maria@email.com",
            "(11) 91234-5678",
            45,
            "F",
            "São Paulo",
            "SP",
            "130/85",
            78,
            36.5,
            98,
            "Hipertensão",
            "Losartana",
            "2025-01-15 10:30:00",
        ),
        (
            "PAC-100002",
            "987.654.321-00",
            "João Santos",
            "joao@email.com",
            "(21) 98765-4321",
            62,
            "M",
            "Rio de Janeiro",
            "RJ",
            "160/100",
            92,
            37.2,
            95,
            "Diabetes Tipo 2",
            "Metformina",
            "2025-01-15 11:00:00",
        ),
        (
            "PAC-100003",
            "111.222.333-44",
            "Ana Costa",
            "ana@email.com",
            "(31) 99999-1234",
            28,
            "F",
            "Belo Horizonte",
            "MG",
            "120/80",
            70,
            38.5,
            97,
            "COVID-19",
            "Paracetamol",
            "2025-01-15 12:00:00",
        ),
        (
            "PAC-100004",
            "555.666.777-88",
            "Pedro Lima",
            "pedro@email.com",
            "(41) 91111-2222",
            55,
            "M",
            "Curitiba",
            "PR",
            "85/55",
            55,
            36.8,
            99,
            "Check-up Rotina",
            None,
            "2025-01-15 13:00:00",
        ),
        (
            "PAC-100001",
            "123.456.789-00",
            "Maria Silva",
            "maria@email.com",
            "(11) 91234-5678",
            45,
            "F",
            "São Paulo",
            "SP",
            "130/85",
            78,
            36.5,
            98,
            "Hipertensão",
            "Losartana",
            "2025-01-15 10:30:00",
        ),
    ]

    columns = [
        "patient_id",
        "cpf",
        "nome",
        "email",
        "telefone",
        "idade",
        "sexo",
        "cidade",
        "estado",
        "blood_pressure",
        "heart_rate",
        "temperatura",
        "saturacao_o2",
        "diagnostico",
        "medicamento",
        "timestamp",
    ]
    return spark.createDataFrame(data, columns)


class TestBatchTransformations:
    """Testes para o módulo de transformações batch."""

    def test_clean_removes_duplicates(self, spark, sample_data):
        from src.data_processing.batch_transformations import DataTransformer

        transformer = DataTransformer()
        cleaned = transformer.clean(sample_data)
        assert cleaned.count() == 4

    def test_enrich_healthcare_adds_columns(self, spark, sample_data):
        from src.data_processing.batch_transformations import DataTransformer

        transformer = DataTransformer()
        enriched = transformer.enrich_healthcare(sample_data)
        assert "sistolica" in enriched.columns
        assert "diastolica" in enriched.columns
        assert "categoria_pressao" in enriched.columns
        assert "categoria_fc" in enriched.columns
        assert "febre" in enriched.columns

    def test_blood_pressure_categorization(self, spark, sample_data):
        from src.data_processing.batch_transformations import DataTransformer

        transformer = DataTransformer()
        enriched = transformer.enrich_healthcare(sample_data)

        categories = {
            row["patient_id"]: row["categoria_pressao"]
            for row in enriched.select("patient_id", "categoria_pressao").distinct().collect()
        }
        assert categories["PAC-100002"] == "alta"
        assert categories["PAC-100003"] == "normal"
        assert categories["PAC-100004"] == "baixa"

    def test_fever_detection(self, spark, sample_data):
        from src.data_processing.batch_transformations import DataTransformer

        transformer = DataTransformer()
        enriched = transformer.enrich_healthcare(sample_data)

        fever = {row["patient_id"]: row["febre"] for row in enriched.select("patient_id", "febre").distinct().collect()}
        assert fever["PAC-100003"] is True
        assert fever["PAC-100001"] is False

    def test_heart_rate_categorization(self, spark, sample_data):
        from src.data_processing.batch_transformations import DataTransformer

        transformer = DataTransformer()
        enriched = transformer.enrich_healthcare(sample_data)

        categories = {
            row["patient_id"]: row["categoria_fc"]
            for row in enriched.select("patient_id", "categoria_fc").distinct().collect()
        }
        assert categories["PAC-100004"] == "bradicardia"
        assert categories["PAC-100001"] == "normal"

    def test_full_transform(self, spark, sample_data):
        from src.data_processing.batch_transformations import DataTransformer

        transformer = DataTransformer()
        result = transformer.transform(sample_data, mask=True)
        assert result.count() == 4
        assert "sistolica" in result.columns
        assert "processing_time" in result.columns


class TestDataMasking:
    """Testes PySpark para mascaramento LGPD."""

    def test_cpf_masking(self, spark, sample_data):
        from src.security.data_masking import DataMasker

        masker = DataMasker()
        masked = masker.mask_data(sample_data, "cpf")
        cpf_values = [row["cpf"] for row in masked.select("cpf").collect()]
        for cpf in cpf_values:
            assert cpf.startswith("***")
            assert cpf.endswith("***")

    def test_nome_pseudonymization(self, spark, sample_data):
        from src.security.data_masking import DataMasker

        masker = DataMasker()
        masked = masker.mask_data(sample_data, "nome")
        names = [row["nome"] for row in masked.select("nome").collect()]
        for name in names:
            assert "***_" in name
            assert len(name) > 5

    def test_email_masking(self, spark, sample_data):
        from src.security.data_masking import DataMasker

        masker = DataMasker()
        masked = masker.mask_data(sample_data, "email")
        emails = [row["email"] for row in masked.select("email").collect()]
        for email in emails:
            assert "***@" in email
            assert "." in email

    def test_patient_id_hashing(self, spark, sample_data):
        from src.security.data_masking import DataMasker

        masker = DataMasker()
        masked = masker.mask_data(sample_data, "patient_id")
        ids = [row["patient_id"] for row in masked.select("patient_id").collect()]
        for pid in ids:
            assert not pid.startswith("PAC-")
            assert len(pid) == 64

    def test_telefone_masking(self, spark, sample_data):
        from src.security.data_masking import DataMasker

        masker = DataMasker()
        masked = masker.mask_data(sample_data, "telefone")
        phones = [row["telefone"] for row in masked.select("telefone").collect()]
        for phone in phones:
            assert phone.startswith("(**) *****-")


class TestDataLake:
    """Testes para o gerenciamento do Data Lake."""

    def test_save_and_read_bronze(self, spark, sample_data):
        from src.data_storage.data_lake import DataLakeManager

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "bronze": tmpdir,
                "silver": tmpdir,
                "gold": tmpdir,
                "checkpoints": os.path.join(tmpdir, "cp"),
            }
            lake = DataLakeManager(spark, config)

            df = sample_data.withColumn("date", F.to_date("timestamp"))
            lake.save_bronze(df, "test_patients", ["date"])
            read_back = lake.read_layer("bronze", "test_patients")
            assert read_back.count() == sample_data.count()

    def test_save_gold_aggregation(self, spark, sample_data):
        from src.data_storage.data_lake import DataLakeManager

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "bronze": tmpdir,
                "silver": tmpdir,
                "gold": tmpdir,
                "checkpoints": os.path.join(tmpdir, "cp"),
            }
            lake = DataLakeManager(spark, config)

            agg = sample_data.groupBy("diagnostico").agg(F.count("*").alias("total"))
            lake.save_gold(agg, "test_agg")
            read_back = lake.read_layer("gold", "test_agg")
            assert read_back.count() > 0


class TestMetrics:
    """Testes para o módulo de métricas e observabilidade."""

    def test_add_metrics(self, spark, sample_data):
        from src.monitoring.metrics import PipelineMetrics

        metrics = PipelineMetrics(spark)
        with_metrics = metrics.add_metrics(sample_data)
        assert "processing_lag_ms" in with_metrics.columns
        assert "record_size_bytes" in with_metrics.columns

    def test_data_quality_check(self, spark, sample_data):
        from src.monitoring.metrics import PipelineMetrics

        metrics = PipelineMetrics(spark)
        report = metrics.check_data_quality(sample_data, "TEST")
        assert report["total_records"] == 5
        assert report["columns"] == 16
        assert "medicamento" in report["null_columns"]

    def test_timer(self, spark):
        import time
        from src.monitoring.metrics import PipelineMetrics

        metrics = PipelineMetrics(spark)
        with metrics.timer("test_stage"):
            time.sleep(0.1)
        timing = metrics.get_timing_report()
        assert "test_stage" in timing
        assert timing["test_stage"] >= 0.1
