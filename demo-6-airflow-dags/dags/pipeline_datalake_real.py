"""
DAG real — pipeline_datalake_real

Orquestra, de verdade, a etapa em lote do pipeline construído ao longo do
curso: ingerir a staging e gerar o processing (mesmos scripts da demo 2,
agora rodando via spark-submit local dentro do próprio container do
Airflow), validar a qualidade do resultado (DuckDB, consultando o parquet
direto no MinIO) e exportar o processing para o Data Warehouse (mesmo
script da demo 3).

A ingestão via CDC (demo 1, Kafka Connect + Debezium) continua sendo
contínua/streaming e não faz parte deste DAG — o Airflow aqui orquestra
somente a parte em lote (Spark + Data Warehouse), que é o padrão comum na
prática: streaming alimenta o raw continuamente, e um job agendado
processa e disponibiliza esse dado periodicamente.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# PySpark 4.0 embute Hadoop 3.4.1, que migrou do AWS SDK V1 para V2.
# Use estes pacotes aqui (demo 6); demo 2 continua com hadoop-aws:3.3.4
# porque usa apache/spark-py:v3.4.0 (Hadoop 3.3.4 embutido).
PACKAGES = "org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6"


def validar_qualidade_datalake():
    import duckdb

    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql(
        """
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='admin';
        SET s3_secret_access_key='admin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        """
    )

    total = con.sql(
        "SELECT COUNT(*) FROM read_parquet('s3://datalake/processing/customers/*.parquet')"
    ).fetchone()[0]
    print(f"Clientes em processing/customers: {total}")
    assert total > 0, "Camada processing de customers está vazia — algo falhou na transformação"

    nulos_email = con.sql(
        "SELECT COUNT(*) FROM read_parquet('s3://datalake/processing/customers/*.parquet') WHERE email IS NULL"
    ).fetchone()[0]
    print(f"E-mails nulos: {nulos_email}")
    assert nulos_email == 0, "Camada processing não deveria ter e-mails nulos"

    duplicados = con.sql(
        "SELECT COUNT(*) - COUNT(DISTINCT id) FROM read_parquet('s3://datalake/processing/customers/*.parquet')"
    ).fetchone()[0]
    print(f"Clientes duplicados (mesmo id aparecendo mais de uma vez): {duplicados}")
    assert duplicados == 0, "Camada processing não deveria ter mais de uma linha por cliente"

    print("Validação de qualidade: OK")


with DAG(
    dag_id="pipeline_datalake_real",
    description="Pipeline real do curso: ingerir (Spark) -> transformar (Spark) -> validar (DuckDB) -> exportar (DW)",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",  # todos os dias às 6h
    catchup=False,
    tags=["ppi-softex", "data-lakes"],
) as dag:

    task_ingerir = BashOperator(
        task_id="ingerir_staging",
        bash_command=f"spark-submit --packages {PACKAGES} /opt/airflow/scripts/datalake/ingest_staging.py",
    )

    task_transformar = BashOperator(
        task_id="transformar_processing",
        bash_command=f"spark-submit --packages {PACKAGES} /opt/airflow/scripts/datalake/transform_processing.py",
    )

    task_validar = PythonOperator(
        task_id="validar_qualidade",
        python_callable=validar_qualidade_datalake,
    )

    task_exportar = BashOperator(
        task_id="exportar_data_warehouse",
        bash_command="python /opt/airflow/scripts/warehouse/export_to_dw.py",
    )

    task_ingerir >> task_transformar >> task_validar >> task_exportar
