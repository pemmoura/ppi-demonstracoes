"""
Roteiro 2 — Ingerindo na camada staging

Lê os eventos CDC que chegaram no bucket "raw" do MinIO
(raw/cdc/dbserver1.inventory.customers/, gravados pela demo 1 via Kafka
Connect + Debezium + S3 Sink) e salva sem nenhuma transformação na camada
staging do Data Lake (bucket "datalake").

A ideia da staging é ser uma cópia fiel da origem: nada é limpo, corrigido
ou tratado aqui. Cada linha é um evento CDC bruto, exatamente como saiu da
demo 1 (campos "before", "after", "op", "ts_ms").

Rodar dentro do container do Spark:
    docker exec -it dl-spark spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /scripts/ingest_staging.py
"""
from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
RAW_BUCKET = "raw"
DATALAKE_BUCKET = "datalake"

spark = (
    SparkSession.builder.appName("ingest-staging")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

print("=" * 70)
print("CUSTOMERS (streaming/CDC) — s3a://raw/cdc/dbserver1.inventory.customers/")
print("=" * 70)

customers_path = f"s3a://{RAW_BUCKET}/cdc/dbserver1.inventory.customers/"
try:
    customers = spark.read.json(customers_path)
    customers.show(truncate=False)
    print("Total de eventos CDC recebidos:", customers.count())
    print("Estrutura do evento Debezium (before/after/op/ts_ms):")
    customers.printSchema()

    customers.write.mode("overwrite").parquet(f"s3a://{DATALAKE_BUCKET}/staging/customers")
    print(f"\nDados salvos em s3a://{DATALAKE_BUCKET}/staging/customers")
except Exception as exc:  # pragma: no cover - mensagem didática
    print(
        "\nNenhum evento CDC encontrado em raw/cdc/. Rode a demo 1 "
        "(demo-1-kafka-connect) e o generate_data.py antes desta etapa.\n"
        f"Detalhe do erro: {exc}"
    )

spark.stop()
