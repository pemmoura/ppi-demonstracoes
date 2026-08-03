"""
Demo 8 — Dia 3, Bloco 2: Revisão e boas práticas

Duas coisas: (1) o fluxo de testar com uma amostra pequena antes de rodar
tudo, e (2) broadcast join x shuffle join -- por que produtos.json (uma
tabela pequena) some da rede quando o Spark faz broadcast dela para cada
executor, em vez de embaralhar (shuffle) as duas tabelas pela rede.

Usa a Spark UI (http://localhost:4040, disponível enquanto o script roda)
para mostrar jobs e stages disparados pelas ações.

Rodar dentro do container Spark:
    docker exec -it spark-course spark-submit /curso/demo-8-boas-praticas/scripts/demo_boas_praticas.py

Depois que os jobs rodam, o script fica esperando (sem consumir CPU) até
que você aperte Ctrl+C -- é o tempo para abrir a Spark UI e conferir
jobs/stages/tasks antes do SparkContext ser encerrado. Não usamos input()
para essa pausa porque o spark-submit não garante que o stdin do terminal
chegue até o processo Python do driver (o mesmo problema resolvido na
demo 1); Ctrl+C, por ser um sinal (SIGINT) e não uma leitura de stdin,
funciona de forma confiável mesmo nesse cenário.
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo-8-boas-praticas").getOrCreate()

pedidos = spark.read.csv("/curso/common/dados/pedidos_grande.csv", header=True, inferSchema=True)
produtos = spark.read.json("/curso/common/dados/produtos.json").dropDuplicates(["produto_id"])

print("=" * 70)
print("Passo 1 — Testar a lógica em uma amostra pequena (.limit(5))")
print("=" * 70)
amostra = pedidos.limit(5)
amostra.show()

transformacao = amostra.withColumn("valor_com_desconto", F.round(F.col("valor_total") * 0.9, 2))
transformacao.show()

print("=" * 70)
print("Passo 2 — explain(): plano de execução que o Spark vai seguir")
print("=" * 70)
transformacao.explain()

print("=" * 70)
print("Passo 3 — Lógica validada, agora rodar no dataset completo")
print("=" * 70)
resultado_completo = pedidos.withColumn("valor_com_desconto", F.round(F.col("valor_total") * 0.9, 2))
print("Total de linhas processadas:", resultado_completo.count())
resultado_completo.show(5)

print("=" * 70)
print("Passo 4 — Shuffle join: desligando o broadcast automático")
print("=" * 70)
print("produtos.json tem", produtos.count(), "linhas -- pequena o bastante")
print("para o Spark normalmente decidir sozinho fazer broadcast dela.")
print("Aqui forçamos o caminho mais caro (SortMergeJoin/shuffle) para comparar:")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # desliga o broadcast automático
inicio = time.time()
join_com_shuffle = pedidos.join(produtos, on="produto_id", how="inner")
total_shuffle = join_com_shuffle.count()
tempo_shuffle = time.time() - inicio
join_com_shuffle.explain()
print(f"Shuffle join: {total_shuffle} linhas em {tempo_shuffle:.3f}s")

print("=" * 70)
print("Passo 5 — Broadcast join explícito: F.broadcast(produtos)")
print("=" * 70)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # volta ao padrão (10MB)
inicio = time.time()
join_com_broadcast = pedidos.join(F.broadcast(produtos), on="produto_id", how="inner")
total_broadcast = join_com_broadcast.count()
tempo_broadcast = time.time() - inicio
join_com_broadcast.explain()
print(f"Broadcast join: {total_broadcast} linhas em {tempo_broadcast:.3f}s")

print("=" * 70)
print("Comparando os planos: no shuffle join aparece SortMergeJoin/Exchange")
print("(dado embaralhado pela rede); no broadcast join aparece")
print("BroadcastHashJoin/BroadcastExchange (produtos.json copiado inteiro")
print("para cada executor, sem embaralhar os pedidos).")
print("=" * 70)

print("=" * 70)
print("Jobs concluídos. Abra http://localhost:4040 para explorar a Spark UI")
print("(abas Jobs, Stages e Tasks) enquanto o SparkContext segue de pé.")
print("Pressione Ctrl+C quando terminar de explorar, para encerrar o script.")
print("=" * 70)
try:
    while True:
        time.sleep(2)
except KeyboardInterrupt:
    print("\nEncerrando por Ctrl+C.")

spark.stop()
