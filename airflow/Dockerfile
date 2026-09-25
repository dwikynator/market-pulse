FROM apache/airflow:3.3.2-python3.12

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "pytest==8.4.2" \
    "uv==0.12.17"

WORKDIR /opt/project

COPY --chown=airflow:root pyproject.toml uv.lock README.md ./
COPY --chown=airflow:root src ./src

RUN uv sync --frozen --no-dev --no-install-package pyspark
