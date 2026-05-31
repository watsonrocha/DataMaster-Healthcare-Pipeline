FROM eclipse-temurin:17-jre-jammy AS base

# ── System dependencies ─────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.11 python3.11-venv python3-pip curl && \
    rm -rf /var/lib/apt/lists/* && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1

ENV JAVA_HOME=/opt/java/openjdk
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# ── Python dependencies ─────────────────────────────────────────────
WORKDIR /opt/pipeline

COPY requirements.txt requirements.prod.txt ./
RUN pip install --no-cache-dir -r requirements.prod.txt

# ── Application code ────────────────────────────────────────────────
COPY DTM/ DTM/
COPY tests/ tests/

# ── Output directory ────────────────────────────────────────────────
RUN mkdir -p /opt/pipeline/output/data_lake/{bronze,silver,gold,checkpoints}

# ── Healthcheck ─────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import pyspark; print('OK')" || exit 1

# ── Default entrypoint ──────────────────────────────────────────────
WORKDIR /opt/pipeline/DTM/DTM
ENTRYPOINT ["python", "main.py"]
