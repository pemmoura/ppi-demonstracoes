"""
Demo 3 — Dia 1, Bloco 3: Introdução aos DataFrames

Carrega as 3 fontes estruturadas da loja -- clientes (CSV), produtos
(JSON) e pedidos (CSV) -- e mostra schema, select, filter e withColumn em
cada uma. Ainda sem join (isso fica para a demo 4): aqui o objetivo é só
conhecer cada fonte separadamente e notar que os pedidos só têm
cliente_id/produto_id, não o nome do cliente ou do produto.

Rodar dentro do container Spark:
    docker exec -it spark-course spark-submit /curso/demo-3-dataframes-intro/scripts/demo_dataframes_intro.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo-3-dataframes-intro").getOrCreate()

print("=" * 70)
print("Fonte 1 — clientes.csv (CSV)")
print("=" * 70)
clientes = spark.read.csv("/curso/common/dados/clientes.csv", header=True, inferSchema=True)
clientes.printSchema()
clientes.select("nome", "cidade", "segmento").show(5)

print("=" * 70)
print("Fonte 2 — produtos.json (JSON)")
print("=" * 70)
produtos = spark.read.json("/curso/common/dados/produtos.json")
produtos.printSchema()
produtos.filter(produtos.categoria == "Informática").show(10, truncate=False)

print("=" * 70)
print("Fonte 3 — pedidos_pequeno.csv (CSV)")
print("=" * 70)
pedidos = spark.read.csv("/curso/common/dados/pedidos_pequeno.csv", header=True, inferSchema=True)
pedidos.printSchema()
pedidos.describe().show()

pedidos_com_valor_unitario = pedidos.withColumn(
    "valor_unitario", F.round(pedidos.valor_total / pedidos.quantidade, 2)
)
pedidos_com_valor_unitario.show()

print("=" * 70)
print("Reparem: pedidos só tem cliente_id e produto_id, não o nome de")
print("nenhum dos dois -- para isso, é preciso fazer join (próxima demo).")
print("=" * 70)

spark.stop()
