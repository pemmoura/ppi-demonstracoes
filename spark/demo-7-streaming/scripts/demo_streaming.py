"""
Demo 7 — Dia 3, Bloco 1: Introdução ao streaming

Monitora uma pasta que recebe, aos poucos, arquivos de um log de aplicação
(simulados por simular_chegada_arquivos.py, em outro terminal) -- o caso
de uso clássico de streaming: "ler continuamente o que chega numa pasta"
(slide "Fontes comuns de streaming").

Cada linha do log só tem pedido_id/evento/nivel -- para saber a categoria
do produto e o nome do cliente por trás de cada evento, o stream é
enriquecido em tempo real com um "stream-static join": o lado que streama
(o log) é cruzado com DataFrames estáticos já carregados (pedidos,
produtos, clientes).

Rodar dentro do container Spark (em um terminal):
    docker exec -it spark-course spark-submit /curso/demo-7-streaming/scripts/demo_streaming.py

E, em outro terminal, simular a chegada do log:
    docker exec -it spark-course python3 /curso/demo-7-streaming/scripts/simular_chegada_arquivos.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo-7-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # streaming reavalia a pasta a cada micro-lote e loga em INFO

PASTA_ENTRADA = "/curso/demo-7-streaming/pasta_entrada"
CHECKPOINT = "/curso/demo-7-streaming/checkpoint"

# readStream exige que a pasta já exista (diferente do read em modo batch).
Path(PASTA_ENTRADA).mkdir(parents=True, exist_ok=True)

print("=" * 70)
print("Carregando as fontes ESTÁTICAS usadas para enriquecer o stream")
print("=" * 70)
pedidos = spark.read.csv("/curso/common/dados/pedidos_pequeno.csv", header=True, inferSchema=True)
produtos = spark.read.json("/curso/common/dados/produtos.json").dropDuplicates(["produto_id"])
clientes = spark.read.csv("/curso/common/dados/clientes.csv", header=True, inferSchema=True)

pedido_para_categoria_cliente = (
    pedidos
    .join(produtos, on="produto_id", how="left")
    .join(clientes, on="cliente_id", how="left")
    .select(pedidos.id.alias("pedido_id"), "categoria", "nome")
)

print("=" * 70)
print(f"readStream: monitorando {PASTA_ENTRADA} por novos arquivos de log")
print("(fonte de texto puro -- spark.readStream.text, sem schema explícito)")
print("=" * 70)
stream_bruto = spark.readStream.text(PASTA_ENTRADA)

stream_parseado = stream_bruto.select(
    F.regexp_extract("value", r"pedido_id=(\d+)", 1).cast("int").alias("pedido_id"),
    F.regexp_extract("value", r"^\S+ \S+ (\S+)\s+pedido_id=", 1).alias("nivel"),
    F.regexp_extract("value", r"evento=(\w+)", 1).alias("evento"),
)

print("=" * 70)
print("Stream-static join: cada linha do log é enriquecida com categoria e cliente")
print("=" * 70)
stream_enriquecido = stream_parseado.join(pedido_para_categoria_cliente, on="pedido_id", how="left")

# Conta TODOS os eventos por categoria (não só os de erro): com poucas
# linhas de log, um evento de erro específico pode demorar vários
# arquivos para aparecer -- contar tudo garante uma tabela não-vazia já
# no primeiro arquivo, e a coluna qtd_erros ainda mostra o que interessa.
contagem_por_categoria = (
    stream_enriquecido
    .groupBy("categoria")
    .agg(
        F.count("*").alias("qtd_eventos"),
        F.sum(F.when(F.col("nivel") == "ERROR", 1).otherwise(0)).alias("qtd_erros"),
    )
)

print("=" * 70)
print("writeStream: eventos e erros por categoria, atualizados a cada novo lote")
print("Pressione Ctrl+C para encerrar.")
print("=" * 70)
query = (
    contagem_por_categoria.writeStream
    .outputMode("complete")
    .format("console")
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

query.awaitTermination()
