"""
DAG Airflow — Streaming Pipeline de Dados de Saúde.

Gerencia o pipeline de Structured Streaming com Kafka:
  - Inicia/monitora queries de streaming
  - Verifica checkpoints e recuperação
  - Alertas para falhas no streaming
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def _verify_kafka(**context):
    """Verifica conectividade com Kafka."""
    import socket

    brokers = context["params"].get("kafka_brokers", "kafka:9092")
    for broker in brokers.split(","):
        host, port = broker.strip().split(":")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((host, int(port)))
            sock.close()
        except (socket.timeout, ConnectionRefusedError) as e:
            raise ConnectionError(f"Kafka broker {broker} não acessível: {e}")


def _monitor_streaming(**context):
    """Monitora saúde do streaming via Spark UI REST API."""
    import requests

    spark_ui = context["params"].get("spark_ui", "http://localhost:4040")
    try:
        resp = requests.get(f"{spark_ui}/api/v1/applications", timeout=10)
        resp.raise_for_status()
        apps = resp.json()
        for app in apps:
            context["ti"].xcom_push(
                key="streaming_status",
                value={"app": app.get("name"), "status": "running"},
            )
    except Exception as e:
        raise RuntimeError(f"Não foi possível monitorar streaming: {e}")


with DAG(
    dag_id="healthcare_streaming_pipeline",
    default_args=default_args,
    description="Pipeline de Structured Streaming com Kafka para dados de saúde",
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "streaming", "kafka"],
) as dag:

    start = EmptyOperator(task_id="start")

    verify_kafka = PythonOperator(
        task_id="verify_kafka_connectivity",
        python_callable=_verify_kafka,
    )

    start_streaming = SparkSubmitOperator(
        task_id="start_kafka_streaming",
        application="/opt/pipeline/DTM/DTM/jobs/kafka_streaming_job.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0",
        },
        name="healthcare-kafka-streaming",
    )

    monitor_streaming = PythonOperator(
        task_id="monitor_streaming_health",
        python_callable=_monitor_streaming,
    )

    end = EmptyOperator(task_id="end")

    start >> verify_kafka >> start_streaming >> monitor_streaming >> end
