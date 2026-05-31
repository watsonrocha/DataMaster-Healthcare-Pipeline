"""
DAG Airflow — Pipeline de Dados de Saúde.

Orquestra o pipeline ETL completo com as etapas:
  1. Extração (dados simulados + APIs públicas)
  2. Ingestão batch (Bronze)
  3. Transformação (Bronze -> Silver)
  4. Agregação (Silver -> Gold)
  5. Salvamento no PostgreSQL
  6. Verificação de qualidade

Schedule: diário às 02:00 UTC
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["alerts@healthcare-pipeline.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def _extract_api_data(**context):
    """Extrai dados de APIs públicas (IBGE + COVID-19)."""
    import sys

    sys.path.insert(0, "/opt/pipeline/DTM/DTM")
    from src.data_extraction.api_extractor import fetch_all_api_data, save_api_data

    api_data = fetch_all_api_data()
    paths = save_api_data(api_data, "/opt/pipeline/output/data_lake/bronze")
    context["ti"].xcom_push(key="api_paths", value=paths)
    return paths


def _check_postgres_available(**context):
    """Verifica se PostgreSQL está disponível."""
    import psycopg2

    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="healthcare",
            user="pipeline",
            password="pipeline123",
            connect_timeout=5,
        )
        conn.close()
        return "save_to_postgres"
    except Exception:
        return "skip_postgres"


with DAG(
    dag_id="healthcare_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL de dados de saúde (Bronze -> Silver -> Gold)",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "etl", "data-pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. Extração de dados simulados ───────────────────────────────
    extract_simulated = SparkSubmitOperator(
        task_id="extract_simulated_data",
        application="/opt/pipeline/DTM/DTM/jobs/extract_simulated.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        name="extract-simulated-data",
        verbose=False,
    )

    # ── 1b. Extração de APIs públicas ────────────────────────────────
    extract_apis = PythonOperator(
        task_id="extract_api_data",
        python_callable=_extract_api_data,
    )

    # ── 2. Ingestão batch (Bronze) ───────────────────────────────────
    ingest_bronze = SparkSubmitOperator(
        task_id="ingest_to_bronze",
        application="/opt/pipeline/DTM/DTM/jobs/ingest_bronze.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        name="ingest-bronze",
    )

    # ── 3. Transformação (Bronze -> Silver) ──────────────────────────
    transform_silver = SparkSubmitOperator(
        task_id="transform_to_silver",
        application="/opt/pipeline/DTM/DTM/jobs/transform_silver.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        name="transform-silver",
    )

    # ── 4. Agregação (Silver -> Gold) ────────────────────────────────
    aggregate_gold = SparkSubmitOperator(
        task_id="aggregate_to_gold",
        application="/opt/pipeline/DTM/DTM/jobs/aggregate_gold.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        name="aggregate-gold",
    )

    # ── 5. Salvamento no PostgreSQL ──────────────────────────────────
    check_postgres = BranchPythonOperator(
        task_id="check_postgres",
        python_callable=_check_postgres_available,
    )

    save_to_postgres = SparkSubmitOperator(
        task_id="save_to_postgres",
        application="/opt/pipeline/DTM/DTM/jobs/save_postgres.py",
        conn_id="spark_default",
        name="save-postgres",
    )

    skip_postgres = EmptyOperator(task_id="skip_postgres")

    # ── 6. Verificação de qualidade ──────────────────────────────────
    quality_check = SparkSubmitOperator(
        task_id="quality_check",
        application="/opt/pipeline/DTM/DTM/jobs/quality_check.py",
        conn_id="spark_default",
        name="quality-check",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── DAG Flow ─────────────────────────────────────────────────────
    start >> [extract_simulated, extract_apis]
    [extract_simulated, extract_apis] >> ingest_bronze
    ingest_bronze >> transform_silver >> aggregate_gold
    aggregate_gold >> check_postgres >> [save_to_postgres, skip_postgres]
    [save_to_postgres, skip_postgres] >> quality_check >> end
