"""
Demo 6 — Dia 2, Bloco 3: Leitura e escrita de dados

Diferente da demo 3 conversão simples, aqui as 4 fontes já nascem em
formatos diferentes de verdade -- CSV (clientes, pedidos), JSON (produtos)
e log de texto (eventos) -- e o Spark lê todas com a mesma família de API
(spark.read...). Depois de juntar tudo (join + agregação do log), grava o
resultado final particionado em Parquet.

Rodar dentro do container Spark:
    docker exec -it spark-course spark-submit /curso/demo-6-leitura-escrita/scripts/demo_formatos.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo-6-leitura-escrita").getOrCreate()

SAIDA_PARQUET = "/curso/demo-6-leitura-escrita/saida/pedidos_enriquecidos"

print("=" * 70)
print("Lendo as 4 fontes, cada uma no seu formato nativo")
print("=" * 70)
clientes = spark.read.csv("/curso/common/dados/clientes.csv", header=True, inferSchema=True)
pedidos = spark.read.csv("/curso/common/dados/pedidos_pequeno.csv", header=True, inferSchema=True)
produtos = spark.read.json("/curso/common/dados/produtos.json").dropDuplicates(["produto_id"])

# Log de texto: sem leitor dedicado, então lemos como texto puro
# (spark.read.text, uma coluna "value" por linha) e extraímos os campos
# com regexp_extract -- o mesmo log que a demo 2 parseou "na mão" com RDD,
# agora com funções nativas de DataFrame.
logs_brutos = spark.read.text("/curso/common/dados/eventos_pequeno.log")
logs = logs_brutos.select(
    F.regexp_extract("value", r"pedido_id=(\d+)", 1).cast("int").alias("pedido_id"),
    F.regexp_extract("value", r"^\S+ \S+ (\S+)\s+pedido_id=", 1).alias("nivel"),
    F.regexp_extract("value", r"evento=(\w+)", 1).alias("evento"),
)

print("Schema do log já parseado como DataFrame:")
logs.printSchema()
logs.show(5, truncate=False)

print("=" * 70)
print("Resumo do log por pedido: quantidade de eventos e se teve erro")
print("=" * 70)
resumo_log = logs.groupBy("pedido_id").agg(
    F.count("*").alias("qtd_eventos"),
    F.max(F.when(F.col("nivel") == "ERROR", 1).otherwise(0)).alias("teve_erro"),
)
resumo_log.show()

print("=" * 70)
print("Join final: pedidos + clientes + produtos + resumo do log")
print("=" * 70)
pedidos_enriquecidos = (
    pedidos
    .join(clientes, on="cliente_id", how="inner")
    .join(produtos, on="produto_id", how="inner")
    .join(resumo_log, pedidos.id == resumo_log.pedido_id, how="left")
    .drop("pedido_id")
)
pedidos_enriquecidos.show(truncate=False)

print("=" * 70)
print(f"Salvando em Parquet, particionado por categoria: {SAIDA_PARQUET}")
print("=" * 70)
pedidos_enriquecidos.write.mode("overwrite").partitionBy("categoria").parquet(SAIDA_PARQUET)

print("=" * 70)
print("Reabrindo o Parquet para conferir")
print("=" * 70)
df_parquet = spark.read.parquet(SAIDA_PARQUET)
df_parquet.printSchema()
df_parquet.show(5)

spark.stop()
