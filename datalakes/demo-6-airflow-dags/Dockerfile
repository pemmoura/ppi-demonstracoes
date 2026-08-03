FROM apache/airflow:3.3.0

# Java é necessário para o PySpark (spark-submit) rodar dentro do próprio
# container do Airflow, em modo local — sem precisar acessar outro container.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir "pyspark~=4.0.0" duckdb s3fs sqlalchemy psycopg2-binary
