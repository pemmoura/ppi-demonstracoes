"""
Demo 1 — Dia 1, Bloco 1: Introdução ao Spark

Mostra a criação da SparkSession (o ponto de entrada de qualquer script
Spark) e o funcionamento da execução preguiçosa (lazy evaluation): uma
transformação (withColumn) não roda nada sozinha -- só quando uma ação
(show) é chamada, o Spark de fato processa os dados.

Usa spark.range(), o mesmo exemplo do slide "Primeiro contato com o
PySpark" -- RDD só é apresentado no próximo bloco (Dia 1, Bloco 2), por
isso esta demo evita RDD de propósito e fica só na API de DataFrame.

Roda em dois passos separados (dois comandos), em vez de pausar no meio do
script com input(): o `spark-submit` não garante que o stdin do terminal
chegue até o processo Python do driver (principalmente com `docker exec`),
então um input() no meio do script pode travar mesmo depois de apertar
Enter. Separar em dois comandos deixa a demonstração igualmente clara e
sem depender disso.

Passo 1 — só a transformação, sem nenhuma ação:
    docker exec -it spark-course spark-submit /curso/demo-1-intro-spark/scripts/demo_sparksession_lazy.py

Passo 2 — a mesma transformação, agora seguida da ação (show):
    docker exec -it spark-course spark-submit /curso/demo-1-intro-spark/scripts/demo_sparksession_lazy.py --disparar-acao
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

disparar_acao = "--disparar-acao" in sys.argv

spark = SparkSession.builder.appName("demo-1-intro-spark").getOrCreate()

print("=" * 70)
print("spark.range(5) cria uma sequência de números -- útil para testar rapidamente")
print("=" * 70)
produtos = spark.range(5)  # 5 "IDs" de produto: 0, 1, 2, 3, 4

print("=" * 70)
print("Transformação aplicada (withColumn) — até aqui, NADA foi executado ainda.")
print("O Spark só descreveu o plano: 'para cada id, montar o nome do produto'.")
print("=" * 70)
produtos_com_nome = produtos.withColumn("produto", F.concat(F.lit("Produto "), produtos.id))

if not disparar_acao:
    print("\nScript terminando AQUI, sem chamar show().")
    print("Repare: nenhuma linha foi impressa acima — nada foi processado.")
    print("Rode de novo com --disparar-acao para disparar o show() e ver o resultado.\n")
else:
    print("=" * 70)
    print("Agora sim: show() é uma AÇÃO, e é o que dispara o processamento.")
    print("=" * 70)
    produtos_com_nome.show()

spark.stop()
