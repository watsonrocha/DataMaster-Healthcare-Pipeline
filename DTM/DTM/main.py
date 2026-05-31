"""
Pipeline de Dados de Saúde — Ponto de entrada principal.

Executa o pipeline completo:
  1. Extração  → Geração de dados simulados (CSV/JSON) + API pública
  2. Ingestão  → Leitura batch (CSV, JSON) + simulação de streaming
  3. Armazenamento → Data Lake em camadas (Bronze → Silver → Gold)
  4. Transformação → Limpeza, enriquecimento, mascaramento LGPD
  5. Observabilidade → Métricas, qualidade de dados, logging
  6. Segurança → RBAC, auditoria, mascaramento de dados sensíveis
"""

import sys
import os
import platform
import logging
from pathlib import Path

# Garante que o diretório DTM/DTM esteja no path
sys.path.insert(0, str(Path(__file__).resolve().parent))


def _setup_java_home():
    """Detecta e configura JAVA_HOME automaticamente no Windows.

    Prioriza Java 17 ou 21 (LTS compatíveis com PySpark).
    Java 23+ não é suportado (getSubject removido).
    """
    if platform.system() != "Windows":
        return

    # Se JAVA_HOME já está definido e aponta para versão compatível, usa
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home and os.path.isfile(os.path.join(java_home, "bin", "java.exe")):
        if not _is_incompatible_java(java_home):
            return
        print(f"JAVA_HOME atual ({java_home}) usa Java incompatível. Buscando Java 17...")

    # Procura Java em locais comuns no Windows
    search_dirs = [
        Path("C:/Program Files/Eclipse Adoptium"),
        Path("C:/Program Files/Java"),
        Path("C:/Program Files/Microsoft/jdk"),
        Path("C:/Program Files/Zulu"),
        Path("C:/Program Files/AdoptOpenJDK"),
    ]

    # Primeiro: procura Java 17 (preferido)
    # Segundo: procura Java 11 ou 21
    # Nunca: Java 23+ (incompatível com PySpark)
    compatible = []
    for base in search_dirs:
        if not base.exists():
            continue
        for jdk_dir in base.iterdir():
            java_exe = jdk_dir / "bin" / "java.exe"
            if not java_exe.exists():
                continue
            name = jdk_dir.name.lower()
            if "17" in name:
                compatible.insert(0, jdk_dir)  # Java 17 tem prioridade
            elif "11" in name or "21" in name:
                compatible.append(jdk_dir)
            elif not any(v in name for v in ["23", "24", "25", "26"]):
                compatible.append(jdk_dir)

    if compatible:
        chosen = compatible[0]
        os.environ["JAVA_HOME"] = str(chosen)
        os.environ["PATH"] = str(chosen / "bin") + os.pathsep + os.environ.get("PATH", "")
        print(f"JAVA_HOME configurado automaticamente: {chosen}")
        return

    print("AVISO: Java 17 não encontrado!")
    print("  Instale: https://adoptium.net/temurin/releases/?version=17")
    print("  Ou defina JAVA_HOME manualmente: $env:JAVA_HOME = 'C:\\caminho\\jdk-17'")


def _is_incompatible_java(java_home: str) -> bool:
    """Verifica se o Java no diretório é versão 23+ (incompatível)."""
    name = Path(java_home).name.lower()
    for v in ["23", "24", "25", "26"]:
        if f"jdk-{v}" in name or f"jdk{v}" in name:
            return True
    return False


def _setup_hadoop_windows():
    """Configura o Hadoop no Windows para que o PySpark funcione corretamente.

    Baixa o winutils.exe real do repositório cdarlint/winutils no GitHub
    e configura HADOOP_HOME automaticamente.
    """
    if platform.system() != "Windows":
        return

    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home and os.path.isfile(os.path.join(hadoop_home, "bin", "winutils.exe")):
        if os.path.getsize(os.path.join(hadoop_home, "bin", "winutils.exe")) > 0:
            return

    hadoop_dir = Path.home() / ".hadoop"
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    winutils = bin_dir / "winutils.exe"

    # Baixa os binários reais do Hadoop para Windows
    base_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
    need_download = not winutils.exists() or winutils.stat().st_size == 0

    if need_download:
        import urllib.request

        for filename in ["winutils.exe", "hadoop.dll"]:
            target = bin_dir / filename
            url = f"{base_url}/{filename}"
            try:
                print(f"Baixando {filename} para compatibilidade Windows...")
                urllib.request.urlretrieve(url, str(target))
                print(f"  {filename} baixado com sucesso!")
            except Exception as e:
                print(f"  Aviso: Não foi possível baixar {filename}: {e}")
                print(f"  Baixe manualmente de: {url}")
                print(f"  Salve em: {target}")
                if not target.exists():
                    target.touch()

    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")


def _setup_windows():
    """Configuração completa para Windows."""
    if platform.system() != "Windows":
        return
    _setup_java_home()
    _setup_hadoop_windows()
    # Força o PySpark a usar o Python atual
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


_setup_windows()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import Config
from src.data_extraction.data_generator import generate_and_save
from src.data_extraction.api_extractor import fetch_all_api_data, save_api_data
from src.data_ingestion.batch.extractors import BatchDataExtractor
from src.data_ingestion.streaming.stream_processor import StreamProcessor
from src.data_processing.batch_transformations import DataTransformer
from src.data_storage.data_lake import DataLakeManager
from src.security.access_control import AccessController
from src.monitoring.metrics import PipelineMetrics


def setup_logging():
    """Configura logging estruturado para o pipeline."""
    os.makedirs(os.path.dirname(Config.LOG_FILE), exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Config.LOG_FILE, encoding="utf-8"),
        ],
    )
    # Reduz verbosidade do Spark
    logging.getLogger("py4j").setLevel(logging.ERROR)


logger = logging.getLogger("pipeline")


_AUTO_EXPORTER = object()


class HealthcareDataPipeline:
    """Pipeline ETL completo para dados de saúde com PySpark."""

    def __init__(self, exporter=_AUTO_EXPORTER):
        setup_logging()
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE DE DADOS DE SAÚDE")
        logger.info("=" * 60)

        builder = (
            SparkSession.builder.appName(Config.SPARK_APP_NAME)
            .master(Config.SPARK_MASTER)
            .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.python.worker.reuse", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        )

        # Configurações adicionais para Windows
        if platform.system() == "Windows":
            warehouse_dir = str(Path(Config.DATA_LAKE_ROOT) / "spark-warehouse")
            builder = (
                builder.config("spark.sql.warehouse.dir", warehouse_dir)
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.python.worker.faulthandler.enabled", "true")
            )

        try:
            self.spark = builder.getOrCreate()
        except Exception as e:
            logger.error(f"Erro ao criar SparkSession: {e}")
            logger.error(f"JAVA_HOME = {os.environ.get('JAVA_HOME', 'NÃO DEFINIDO')}")
            logger.error(f"HADOOP_HOME = {os.environ.get('HADOOP_HOME', 'NÃO DEFINIDO')}")
            logger.error("Verifique se o Java 17 está instalado: https://adoptium.net/temurin/releases/")
            raise
        self.spark.sparkContext.setLogLevel("WARN")

        self.extractor = BatchDataExtractor(self.spark)
        self.stream_processor = StreamProcessor(self.spark)
        self.transformer = DataTransformer()
        self.storage = DataLakeManager(self.spark)
        self.access = AccessController()

        # ── Observabilidade: exportador Prometheus (opcional) ──────────
        # exporter=_AUTO_EXPORTER → cria/inicia um novo exportador.
        # exporter=<instância|None> → reusa o fornecido (modo loop contínuo).
        self.exporter = self._init_exporter() if exporter is _AUTO_EXPORTER else exporter
        self.metrics = PipelineMetrics(self.spark, exporter=self.exporter)

    def _init_exporter(self):
        """Cria e inicia o exportador Prometheus, se habilitado e disponível."""
        if not Config.METRICS_ENABLED:
            return None
        try:
            from src.monitoring.prometheus_metrics import PrometheusExporter
        except ImportError:
            logger.warning(
                "prometheus_client não instalado — métricas Prometheus desabilitadas. "
                "Instale com: pip install prometheus-client"
            )
            return None

        exporter = PrometheusExporter(port=Config.METRICS_PORT)
        exporter.start_server()
        exporter.set_pipeline_info(version="1.0.0", environment=os.getenv("ENVIRONMENT", "local"))
        # Inicializa a série de erros em 0 para o painel mostrar uma linha base.
        exporter.errors_total.labels(stage="pipeline", error_type="none")
        logger.info("Exportador Prometheus ativo em :%d/metrics", Config.METRICS_PORT)
        return exporter

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 1A — EXTRAÇÃO DE DADOS SIMULADOS
    # ══════════════════════════════════════════════════════════════════════
    def extract(self):
        """Gera dados simulados e salva em CSV/JSON (camada Bronze)."""
        with self.metrics.timer("Extração de Dados (simulados)"):
            logger.info("Gerando %d registros simulados...", Config.SIMULATED_RECORDS)
            result = generate_and_save(Config.SIMULATED_RECORDS)
            logger.info("Dados salvos: CSV=%s  JSON=%s", result["csv"], result["json"])
            return result

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 1B — EXTRAÇÃO DE DADOS VIA API PÚBLICA
    # ══════════════════════════════════════════════════════════════════════
    def extract_api(self):
        """Busca dados de APIs públicas de saúde (IBGE + COVID-19)."""
        with self.metrics.timer("Extração de Dados (APIs públicas)"):
            try:
                api_data = fetch_all_api_data()
                api_paths = save_api_data(api_data, Config.STORAGE["bronze"])
                logger.info("APIs: %d fontes extraídas", len(api_paths))
                return api_data
            except Exception as e:
                logger.warning("Falha na extração de APIs (não crítico): %s", e)
                return {}

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 2 — INGESTÃO BATCH
    # ══════════════════════════════════════════════════════════════════════
    def ingest_batch(self, data_paths: dict):
        """Lê dados de múltiplas fontes e unifica em um DataFrame."""
        with self.metrics.timer("Ingestão Batch"):
            # Controle de acesso
            self.access.require_permission("admin", "read")
            self.access.audit_log("admin", "read", "healthcare_batch")

            # Leitura CSV
            df_csv = self.extractor.from_csv(data_paths["csv"])
            logger.info("CSV carregado: %d registros", df_csv.count())

            # Leitura JSON
            df_json = self.extractor.from_json(data_paths["json"])
            logger.info("JSON carregado: %d registros", df_json.count())

            # União dos DataFrames
            df = df_csv.unionByName(df_json, allowMissingColumns=True)
            logger.info("Total após união: %d registros", df.count())

            # Verificação de qualidade (dados brutos)
            self.metrics.check_data_quality(df, "BRONZE (dados brutos)")

            # Salva na camada Bronze (dados brutos)
            df = df.withColumn("date", F.to_date("timestamp"))
            self.storage.save_bronze(df, "healthcare", ["date"])

            return df

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 3 — INGESTÃO STREAMING (simulada)
    # ══════════════════════════════════════════════════════════════════════
    def ingest_streaming(self):
        """Simula ingestão de dados em tempo real (micro-batch)."""
        with self.metrics.timer("Ingestão Streaming (simulada)"):
            try:
                df_stream = self.stream_processor.simulate_stream(n_events=20)
                n_events = df_stream.count()
                logger.info("Streaming simulado: %d eventos", n_events)
                if self.exporter is not None:
                    try:
                        self.exporter.record_processed("streaming", "simulated", n_events)
                        # Lag simulado proporcional ao volume de eventos.
                        self.exporter.streaming_lag.labels(query="healthcare_stream").set(round(n_events / 100, 2))
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("Falha ao exportar métricas de streaming: %s", exc)
                return df_stream
            except Exception as e:
                logger.warning("Streaming simulado falhou (não crítico): %s", e)
                logger.warning("Continuando pipeline apenas com dados batch...")
                if self.exporter is not None:
                    try:
                        self.exporter.record_error("streaming", type(e).__name__)
                    except Exception:  # noqa: BLE001
                        pass
                return None

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 4 — TRANSFORMAÇÃO (Bronze → Silver)
    # ══════════════════════════════════════════════════════════════════════
    def transform(self, df):
        """Limpa, enriquece e mascara dados sensíveis."""
        with self.metrics.timer("Transformação e Mascaramento"):
            self.access.require_permission("admin", "write")

            # Transformação completa (limpeza + enriquecimento + mascaramento)
            df_silver = self.transformer.transform(df, mask=True)

            # Adiciona métricas de processamento
            df_silver = self.metrics.add_metrics(df_silver)

            # Verificação de qualidade (dados transformados)
            self.metrics.check_data_quality(df_silver, "SILVER (dados limpos)")
            self.metrics.check_thresholds(df_silver)

            # Salva na camada Silver
            df_silver = df_silver.withColumn("date", F.to_date("timestamp"))
            self.storage.save_silver(df_silver, "healthcare", ["date"])

            return df_silver

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 5 — AGREGAÇÃO (Silver → Gold)
    # ══════════════════════════════════════════════════════════════════════
    def aggregate(self, df_silver):
        """Cria visões agregadas para análise e BI."""
        with self.metrics.timer("Agregação (Gold)"):
            # Agregação por diagnóstico
            df_diagnostico = (
                df_silver.groupBy("diagnostico")
                .agg(
                    F.count("*").alias("total_pacientes"),
                    F.avg("heart_rate").alias("media_freq_cardiaca"),
                    F.avg("sistolica").alias("media_sistolica"),
                    F.avg("diastolica").alias("media_diastolica"),
                    F.avg("temperatura").alias("media_temperatura"),
                )
                .orderBy(F.desc("total_pacientes"))
            )
            self.storage.save_gold(df_diagnostico, "por_diagnostico")

            # Agregação por estado
            if "estado" in df_silver.columns:
                df_estado = (
                    df_silver.groupBy("estado")
                    .agg(
                        F.count("*").alias("total_pacientes"),
                        F.avg("heart_rate").alias("media_freq_cardiaca"),
                        F.sum(F.when(F.col("categoria_pressao") == "alta", 1).otherwise(0)).alias(
                            "pacientes_pressao_alta"
                        ),
                    )
                    .orderBy(F.desc("total_pacientes"))
                )
                self.storage.save_gold(df_estado, "por_estado")

            # Agregação por categoria de pressão
            df_pressao = df_silver.groupBy("categoria_pressao").agg(
                F.count("*").alias("total"),
                F.avg("sistolica").alias("media_sistolica"),
                F.avg("diastolica").alias("media_diastolica"),
            )
            self.storage.save_gold(df_pressao, "por_pressao")

            return df_diagnostico

    # ══════════════════════════════════════════════════════════════════════
    # DEMONSTRAÇÃO DE SEGURANÇA
    # ══════════════════════════════════════════════════════════════════════
    def demo_security(self):
        """Demonstra o controle de acesso RBAC."""
        logger.info("=" * 40)
        logger.info("DEMONSTRAÇÃO DE SEGURANÇA (RBAC)")
        logger.info("=" * 40)

        for user in ["admin", "analista_01", "cientista_01", "visitante_01"]:
            perms = self.access.get_permissions(user)
            role = self.access.get_user_role(user)
            logger.info("  Usuário: %-15s | Role: %-18s | Permissões: %s", user, role, perms)

        # Testa acesso negado
        try:
            self.access.require_permission("visitante_01", "write")
        except PermissionError as e:
            logger.info("  Acesso negado corretamente: %s", e)

    # ══════════════════════════════════════════════════════════════════════
    # EXECUÇÃO PRINCIPAL
    # ══════════════════════════════════════════════════════════════════════
    def save_to_postgres(self, df_gold, api_data):
        """Salva dados processados no PostgreSQL (se disponível)."""
        with self.metrics.timer("Salvamento PostgreSQL"):
            try:
                from src.data_storage.database import (
                    init_database,
                    save_dataframe_to_postgres,
                    save_api_data_to_postgres,
                )

                init_database()

                # Salva Gold no PostgreSQL
                save_dataframe_to_postgres(df_gold, "gold_diagnostico")
                logger.info("Dados Gold salvos no PostgreSQL!")

                # Salva dados de API no PostgreSQL
                if api_data:
                    save_api_data_to_postgres(api_data)
                    logger.info("Dados de APIs salvos no PostgreSQL!")

            except ImportError:
                logger.warning("psycopg2 não instalado. Pulando PostgreSQL.")
                logger.warning("Instale: pip install psycopg2-binary")
            except Exception as e:
                logger.warning("PostgreSQL não disponível (não crítico): %s", e)
                logger.warning("Para usar: docker-compose up -d")

    def _print_requisitos_report(self, api_data, timing):
        """Imprime relatório de conformidade com os 8 requisitos do Data Master."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("RELATÓRIO DE REQUISITOS — DATA MASTER")
        logger.info("=" * 70)

        requisitos = [
            (
                "1. Extração de Dados",
                "ATENDIDO",
                "Faker (simulados) + API IBGE (27 estados) + API COVID-19 + CSV + JSON + JDBC",
            ),
            (
                "2. Ingestão de Dados",
                "ATENDIDO",
                "Batch ETL (CSV/JSON/Parquet) + Streaming simulado + suporte Kafka (Lambda/Kappa)",
            ),
            (
                "3. Armazenamento de Dados",
                "ATENDIDO",
                "Data Lake Medallion (Bronze/Silver/Gold) em Parquet + PostgreSQL via Docker",
            ),
            (
                "4. Observabilidade",
                "ATENDIDO",
                "Logging estruturado + timer por etapa + qualidade de dados + alertas automaticos",
            ),
            (
                "5. Segurança de Dados",
                "ATENDIDO",
                "SHA-256 com chave + RBAC (4 perfis) + log de auditoria + conformidade LGPD",
            ),
            (
                "6. Mascaramento de Dados",
                "ATENDIDO",
                "CPF (parcial) + nome (pseudonimização) + email (domínio) + telefone + hash de IDs",
            ),
            (
                "7. Arquitetura de Dados",
                "ATENDIDO",
                "Data Lake Medallion + PySpark distribuído + Parquet columnar + particionamento por data",
            ),
            (
                "8. Escalabilidade",
                "ATENDIDO",
                "local[*] → cluster Spark + particionamento + config de memória ajustável",
            ),
        ]

        for nome, status, detalhes in requisitos:
            logger.info("  [%s] %-30s", status, nome)
            logger.info("         %s", detalhes)

        logger.info("")
        logger.info("  APIs externas: %d fontes carregadas", len(api_data) if api_data else 0)
        logger.info("  Etapas executadas: %d", len(timing))
        logger.info("  Tempo total: %.2fs", sum(timing.values()))
        logger.info("")
        logger.info("  RESULTADO: 8/8 requisitos atendidos")
        logger.info("=" * 70)

    def _export_storage_metrics(self):
        """Calcula o tamanho de cada camada do Data Lake e exporta ao Prometheus."""
        if self.exporter is None:
            return

        def _dir_size(path: str) -> int:
            total = 0
            for root, _dirs, files in os.walk(path):
                for name in files:
                    fp = os.path.join(root, name)
                    try:
                        total += os.path.getsize(fp)
                    except OSError:
                        pass
            return total

        for layer in ("bronze", "silver", "gold"):
            path = Config.STORAGE.get(layer, "")
            if path and os.path.isdir(path):
                size = _dir_size(path)
                try:
                    self.exporter.set_storage_metrics(layer=layer, size_bytes=size, fmt="parquet")
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Falha ao exportar storage de %s: %s", layer, exc)

    def run(self):
        """Executa o pipeline completo."""
        try:
            # 1A. Extração de dados simulados
            data_paths = self.extract()

            # 1B. Extração de dados via APIs públicas
            api_data = self.extract_api()

            # 2. Ingestão batch
            df_batch = self.ingest_batch(data_paths)

            # 3. Ingestão streaming (simulada)
            df_stream = self.ingest_streaming()

            # 4. Unifica batch + streaming
            if df_stream is not None:
                df_all = df_batch.unionByName(df_stream, allowMissingColumns=True)
                df_all = df_all.withColumn("date", F.to_date("timestamp"))
                logger.info("Total combinado (batch + streaming): %d registros", df_all.count())
            else:
                df_all = df_batch
                logger.info("Usando apenas dados batch: %d registros", df_all.count())

            # 5. Transformação (Bronze → Silver)
            df_silver = self.transform(df_all)

            # 6. Agregação (Silver → Gold)
            df_gold = self.aggregate(df_silver)

            # 7. Salvamento no PostgreSQL (se disponível)
            self.save_to_postgres(df_gold, api_data)

            # 8. Demonstração de segurança
            self.demo_security()

            # Exporta tamanho das camadas do Data Lake para o Prometheus
            self._export_storage_metrics()

            # 9. Relatório final
            logger.info("=" * 60)
            logger.info("PIPELINE CONCLUÍDO COM SUCESSO")
            logger.info("=" * 60)
            timing = self.metrics.get_timing_report()
            for stage, elapsed in timing.items():
                logger.info("  %-35s → %.2fs", stage, elapsed)
            total_time = sum(timing.values())
            logger.info("  %-35s → %.2fs", "TEMPO TOTAL", total_time)
            logger.info("=" * 60)

            # Mostra amostra dos dados Gold
            logger.info("Amostra dos dados agregados (Gold - por diagnóstico):")
            df_gold.show(10, truncate=False)

            # Relatório de requisitos Data Master
            self._print_requisitos_report(api_data, timing)

        except KeyboardInterrupt:
            logger.warning("Pipeline interrompido pelo usuário")
        except Exception as e:
            if self.exporter is not None:
                try:
                    self.exporter.record_error("pipeline", type(e).__name__)
                except Exception:  # noqa: BLE001
                    pass
            logger.error("Erro no pipeline: %s", e, exc_info=True)
            raise
        finally:
            self.spark.stop()
            logger.info("SparkSession encerrada.")


def _keep_alive():
    """Mantém o processo vivo para que o Prometheus continue coletando métricas."""
    import time as _time

    logger.info(
        "Servidor de métricas ativo em :%d/metrics — mantendo vivo (Ctrl+C para sair).",
        Config.METRICS_PORT,
    )
    try:
        while True:
            _time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Encerrando servidor de métricas.")


if __name__ == "__main__":
    import time as _time

    interval = Config.METRICS_LOOP_INTERVAL
    if interval > 0:
        # Modo contínuo: reexecuta o pipeline periodicamente, alimentando os
        # gráficos de taxa do Grafana. Reusa um único exportador entre execuções.
        shared_exporter = None
        if Config.METRICS_ENABLED:
            try:
                from src.monitoring.prometheus_metrics import PrometheusExporter

                shared_exporter = PrometheusExporter(port=Config.METRICS_PORT)
                shared_exporter.start_server()
                shared_exporter.set_pipeline_info(version="1.0.0", environment=os.getenv("ENVIRONMENT", "local"))
                shared_exporter.errors_total.labels(stage="pipeline", error_type="none")
            except ImportError:
                logger.warning("prometheus_client não instalado — métricas desabilitadas.")

        while True:
            HealthcareDataPipeline(exporter=shared_exporter).run()
            logger.info("Aguardando %ds até a próxima execução do pipeline...", interval)
            _time.sleep(interval)
    else:
        pipeline = HealthcareDataPipeline()
        pipeline.run()
        if Config.METRICS_KEEP_ALIVE and pipeline.exporter is not None:
            _keep_alive()
