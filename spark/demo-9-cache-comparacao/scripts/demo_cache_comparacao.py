"""
Demo 9 — Dia 3, Bloco 3: Boas práticas e comparação de abordagens

Responde "qual categoria vendeu mais?" usando RDD, DataFrame e Spark SQL
-- e, como a categoria só existe em produtos.json, as 3 abordagens
precisam fazer o mesmo join (pedidos + produtos) para chegar lá. Depois,
aplica cache() no DataFrame enriquecido (pedidos + clientes + produtos) e
compara o tempo de reaproveitá-lo em várias agregações, com e sem cache.

Rodar dentro do container Spark:
    docker exec -it spark-course spark-submit /curso/demo-9-cache-comparacao/scripts/demo_cache_comparacao.py
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo-9-cache-comparacao").getOrCreate()
sc = spark.sparkContext

print("=" * 70)
print("Abordagem 1 — RDD: join manual (pedidos x produtos) + reduceByKey")
print("=" * 70)
cabecalho_pedidos = sc.textFile("/curso/common/dados/pedidos_pequeno.csv").first()
pedidos_rdd = (
    sc.textFile("/curso/common/dados/pedidos_pequeno.csv")
    .filter(lambda linha: linha != cabecalho_pedidos)
    .map(lambda linha: linha.split(","))
    .map(lambda campos: (campos[2], float(campos[4])))  # (produto_id, valor_total)
)
# RDD.join() é inner join por padrão: o pedido com produto_id inexistente
# (visto na demo 4) simplesmente não aparece no resultado, sem erro.

produtos_df_para_rdd = spark.read.json("/curso/common/dados/produtos.json").dropDuplicates(["produto_id"])
produto_para_categoria = produtos_df_para_rdd.rdd.map(lambda linha: (str(linha["produto_id"]), linha["categoria"]))

valor_por_categoria_rdd = (
    pedidos_rdd.join(produto_para_categoria)  # (produto_id, (valor_total, categoria))
    .map(lambda par: (par[1][1], par[1][0]))  # (categoria, valor_total)
    .reduceByKey(lambda a, b: a + b)
)
top_rdd = valor_por_categoria_rdd.max(key=lambda par: par[1])
print("Categoria que mais vendeu (RDD):", top_rdd)

print("=" * 70)
print("Abordagem 2 — DataFrame: join + groupBy + sum + orderBy")
print("=" * 70)
pedidos_df = spark.read.csv("/curso/common/dados/pedidos_pequeno.csv", header=True, inferSchema=True)
produtos_df = spark.read.json("/curso/common/dados/produtos.json").dropDuplicates(["produto_id"])

top_df = (
    pedidos_df.join(produtos_df, on="produto_id")
    .groupBy("categoria")
    .sum("valor_total")
    .orderBy("sum(valor_total)", ascending=False)
    .first()
)
print("Categoria que mais vendeu (DataFrame):", top_df)

print("=" * 70)
print("Abordagem 3 — Spark SQL: join + GROUP BY + ORDER BY")
print("=" * 70)
pedidos_df.createOrReplaceTempView("pedidos")
produtos_df.createOrReplaceTempView("produtos")
top_sql = spark.sql(
    """
    SELECT pr.categoria, SUM(p.valor_total) AS total
    FROM pedidos p
    JOIN produtos pr ON p.produto_id = pr.produto_id
    GROUP BY pr.categoria
    ORDER BY total DESC
    LIMIT 1
    """
).first()
print("Categoria que mais vendeu (SQL):", top_sql)

print("=" * 70)
print("cache(): reaproveitando o DataFrame enriquecido (pedidos+clientes+produtos)")
print("=" * 70)
clientes_df = spark.read.csv("/curso/common/dados/clientes.csv", header=True, inferSchema=True)
pedidos_grande = spark.read.csv("/curso/common/dados/pedidos_grande.csv", header=True, inferSchema=True)

pedidos_enriquecidos = (
    pedidos_grande
    .join(clientes_df, on="cliente_id", how="inner")
    .join(produtos_df, on="produto_id", how="inner")
)

inicio = time.time()
pedidos_enriquecidos.groupBy("categoria").count().collect()
pedidos_enriquecidos.groupBy("regiao").count().collect()
pedidos_enriquecidos.groupBy("segmento").count().collect()
tempo_sem_cache = time.time() - inicio
print(f"Sem cache, 3 agregações sobre o mesmo join: {tempo_sem_cache:.3f}s")

pedidos_enriquecidos_cache = pedidos_enriquecidos.cache()
pedidos_enriquecidos_cache.count()  # força o cache a ser preenchido antes de medir

inicio = time.time()
pedidos_enriquecidos_cache.groupBy("categoria").count().collect()
pedidos_enriquecidos_cache.groupBy("regiao").count().collect()
pedidos_enriquecidos_cache.groupBy("segmento").count().collect()
tempo_com_cache = time.time() - inicio
print(f"Com cache, as mesmas 3 agregações (join já em memória): {tempo_com_cache:.3f}s")

pedidos_enriquecidos_cache.unpersist()
spark.stop()
