"""
Roteiro 3 — Gerando a camada processing

Lê a camada staging de "customers" (eventos CDC brutos) e aplica uma
limpeza simples, deixando claro o que muda em cada passo:

    staging (evento CDC cru)                ->  processing (tratado)
    ------------------------------------------------------------------
    1 evento por mudança (INSERT/UPDATE)    ->  1 linha por cliente (última versão)
    dado dentro de "after"                  ->  colunas soltas (id, first_name, ...)
    inclui eventos de DELETE ("after"=null) ->  removidos da processing
    'MARIA   SILVA' / '  joão'              ->  'Maria Silva' / 'João' (trim + Proper Case)
    ' GBailey@FOOBAR.COM '                  ->  'gbailey@foobar.com' (trim + minúsculas)

Rodar dentro do container do Spark:
    docker exec -it dl-spark spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /scripts/transform_processing.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET = "datalake"

spark = (
    SparkSession.builder.appName("transform-processing")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

print("=" * 70)
print("CUSTOMERS — ANTES (camada STAGING, evento CDC cru)")
print("=" * 70)

try:
    staging_customers = spark.read.parquet(f"s3a://{BUCKET}/staging/customers")
    staging_customers.show(truncate=False)
    print("Total de eventos (staging):", staging_customers.count())

    # cada evento tem "after" (estado pós-mudança, nulo em DELETE) e "ts_ms";
    # ficamos só com a versão mais recente de cada cliente e descartamos deletes
    janela_por_cliente = Window.partitionBy("after.id").orderBy(F.col("ts_ms").desc())

    processing_customers = (
        staging_customers
        .filter(F.col("after").isNotNull())  # descarta eventos de DELETE
        .withColumn("linha_mais_recente", F.row_number().over(janela_por_cliente))
        .filter(F.col("linha_mais_recente") == 1)
        .select(
            F.col("after.id").alias("id"),
            F.col("after.first_name").alias("first_name"),
            F.col("after.last_name").alias("last_name"),
            F.col("after.email").alias("email"),
            F.col("op").alias("ultima_operacao"),
            F.col("ts_ms").alias("atualizado_em_ms"),
        )
        # 1) remove espacos sobrando (antes/depois e espacos duplos no meio)
        .withColumn("first_name", F.trim(F.regexp_replace("first_name", r"\s+", " ")))
        .withColumn("last_name", F.trim(F.regexp_replace("last_name", r"\s+", " ")))
        .withColumn("email", F.trim(F.regexp_replace("email", r"\s+", "")))
        # 2) padroniza a grafia: nomes em Proper Case, e-mail em minusculas
        .withColumn("first_name", F.initcap("first_name"))
        .withColumn("last_name", F.initcap("last_name"))
        .withColumn("email", F.lower("email"))
        .orderBy("id")
    )

    print("\n" + "=" * 70)
    print("CUSTOMERS — DEPOIS (camada PROCESSING: 1 linha por cliente, dado tratado)")
    print("=" * 70)
    processing_customers.show(truncate=False)
    print("Total de clientes (processing):", processing_customers.count())
    processing_customers.printSchema()

    print("\n" + "-" * 70)
    print("RESUMO DO QUE MUDOU")
    print("-" * 70)
    print(
        f"Eventos CDC (staging): {staging_customers.count()} -> "
        f"Clientes únicos (processing): {processing_customers.count()} "
        "(1 por cliente, só a versão mais recente, deletes removidos)"
    )
    print("Nomes: espacos duplicados/sobrando removidos, grafia padronizada (Proper Case)")
    print("E-mail: espacos removidos, padronizado em minusculas")

    processing_customers.write.mode("overwrite").parquet(f"s3a://{BUCKET}/processing/customers")
    print(f"Dados salvos em s3a://{BUCKET}/processing/customers")
except Exception as exc:  # pragma: no cover - mensagem didática
    print(
        "\nNenhum dado em staging/customers. Rode ingest_staging.py depois de "
        "gerar eventos CDC na demo 1 (demo-1-kafka-connect) com o "
        "generate_data.py.\n"
        f"Detalhe do erro: {exc}"
    )

spark.stop()
