"""
Atividade 8 — Script com um erro proposital

Este script tem um erro de nome de coluna, de propósito, para a atividade
prática: encontrar e corrigir o problema usando a mensagem de erro do
Spark (veja o passo a passo no ROTEIRO_PRATICA_8.md).

Rodar dentro do container Spark (vai falhar até ser corrigido):
    docker exec -it spark-course spark-submit /curso/demo-8-boas-praticas/scripts/script_com_erro.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("script-com-erro").getOrCreate()

df = spark.read.csv("/curso/common/dados/pedidos_grande.csv", header=True, inferSchema=True)

print("Testando a lógica em uma amostra pequena antes de rodar tudo...")
amostra = df.limit(5)

# BUG PROPOSITAL: a coluna se chama "valor_total" no CSV, não "valor".
resultado = amostra.withColumn("valor_com_desconto", F.round(F.col("valor") * 0.9, 2))
resultado.show()

spark.stop()
