"""
Demo 4 — Dia 2, Bloco 1: Operações com DataFrames (joins + agregações)

Este é o bloco em que as 3 fontes da loja finalmente se encontram:
pedidos (CSV) + clientes (CSV) + produtos (JSON). No caminho, mostra:

    1) dropna/fillna -- alguns pedidos não têm cliente_id
    2) por que um join "ingênuo" duplica linhas -- produtos.json tem um
       produto_id cadastrado 2x -- e como resolver com drop_duplicates
    3) o join de verdade, das 3 tabelas
    4) agregações pós-join: groupBy, agg, orderBy, renomeação de colunas
    5) filtros combinados (& / |)

Rodar dentro do container Spark:
    docker exec -it spark-course spark-submit /curso/demo-4-dataframes-agregacoes/scripts/demo_agregacoes.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo-4-dataframes-agregacoes").getOrCreate()

pedidos = spark.read.csv("/curso/common/dados/pedidos_pequeno.csv", header=True, inferSchema=True)
clientes = spark.read.csv("/curso/common/dados/clientes.csv", header=True, inferSchema=True)
produtos = spark.read.json("/curso/common/dados/produtos.json")

print("=" * 70)
print("Passo 1 — Lidando com valores nulos (dropna / fillna)")
print("=" * 70)
print("Total de pedidos:", pedidos.count())
print("Pedidos sem cliente_id (nulo):", pedidos.filter(pedidos.cliente_id.isNull()).count())

pedidos_sem_nulos = pedidos.dropna(subset=["cliente_id"])
print("Total após dropna(subset=['cliente_id']):", pedidos_sem_nulos.count())

pedidos_com_fillna = pedidos.fillna({"cliente_id": -1})
print("Alternativa: fillna({'cliente_id': -1}) mantém a linha, marcando com um id inválido")
pedidos_com_fillna.filter(pedidos_com_fillna.cliente_id == -1).show()

print("=" * 70)
print("Passo 2 — Join mal feito duplica linhas")
print("=" * 70)
print("produtos.json tem", produtos.count(), "linhas -- repare que há um produto_id repetido:")
produtos.groupBy("produto_id").count().filter("count > 1").show()

# Aqui usamos how="left" de propósito, só para isolar o efeito da
# duplicidade: com "inner" (o padrão), o pedido com produto_id inexistente
# (visto no Passo 1) desapareceria do resultado e "cancelaria" visualmente
# o aumento causado pelo produto duplicado -- a contagem ficaria igual e
# pareceria que nada aconteceu, mesmo com a duplicação ocorrendo de fato.
pedidos_x_produtos_ingenuo = pedidos_sem_nulos.join(produtos, on="produto_id", how="left")
print("Pedidos antes do join:", pedidos_sem_nulos.count())
print("Pedidos DEPOIS do join ingênuo (sem tratar duplicidade):", pedidos_x_produtos_ingenuo.count())
print("-> aumentou! o pedido do produto_id=1 apareceu 2x, um para cada fornecedor cadastrado.")

produtos_sem_duplicidade = produtos.dropDuplicates(["produto_id"])
pedidos_x_produtos_corrigido = pedidos_sem_nulos.join(produtos_sem_duplicidade, on="produto_id", how="left")
print("Pedidos DEPOIS do join corrigido (produtos.dropDuplicates(['produto_id'])):", pedidos_x_produtos_corrigido.count())
print("-> voltou ao total original: a duplicidade foi eliminada.")

print("=" * 70)
print("Passo 3 — O join completo: pedidos + clientes + produtos")
print("=" * 70)
# Reconstruído do zero com inner join (não reaproveita o left join do Passo
# 2): para o restante da demo, queremos só pedidos com produto E cliente
# válidos -- um dataset limpo para agregações e filtros.
pedidos_enriquecidos = (
    pedidos_sem_nulos
    .join(produtos_sem_duplicidade, on="produto_id", how="inner")
    .join(clientes, on="cliente_id", how="inner")
    .select(
        "id", "cliente_id", "nome", "cidade", "regiao", "segmento",
        "produto_id", "produto", "categoria", "quantidade", "valor_total",
    )
)
pedidos_enriquecidos.show(10, truncate=False)

print("=" * 70)
print("Passo 4 — Agregações pós-join: total e ticket médio por região")
print("=" * 70)
resultado = (
    pedidos_enriquecidos.groupBy("regiao")
    .agg(
        F.sum("valor_total").alias("total_vendido"),
        F.avg("valor_total").alias("ticket_medio"),
        F.count("*").alias("qtd_pedidos"),
    )
    .orderBy(F.col("total_vendido").desc())
)
resultado.show()

print("=" * 70)
print("Passo 5 — Filtros combinados (& / |): pedidos grandes do Sudeste ou Sul")
print("=" * 70)
pedidos_enriquecidos.filter(
    (pedidos_enriquecidos.valor_total > 200)
    & ((pedidos_enriquecidos.regiao == "Sudeste") | (pedidos_enriquecidos.regiao == "Sul"))
).show(truncate=False)

spark.stop()
