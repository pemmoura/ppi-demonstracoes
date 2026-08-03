"""
Demo 5 — Dia 2, Bloco 2: Spark SQL (básico)

Registra as 3 fontes da loja como views temporárias e mostra um join de
3 tabelas em SQL, junto com os recursos mais usados no dia a dia:
CASE WHEN, funções de data (YEAR, DATEDIFF) e de texto (UPPER, TRIM,
CONCAT). Fecha comparando uma das consultas com o equivalente em
DataFrame API.

Rodar dentro do container Spark:
    docker exec -it spark-course spark-submit /curso/demo-5-spark-sql/scripts/demo_spark_sql.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo-5-spark-sql").getOrCreate()

pedidos = spark.read.csv("/curso/common/dados/pedidos_pequeno.csv", header=True, inferSchema=True)
clientes = spark.read.csv("/curso/common/dados/clientes.csv", header=True, inferSchema=True)
produtos = spark.read.json("/curso/common/dados/produtos.json").dropDuplicates(["produto_id"])

pedidos.createOrReplaceTempView("pedidos")
clientes.createOrReplaceTempView("clientes")
produtos.createOrReplaceTempView("produtos")

print("=" * 70)
print("Join de 3 tabelas em SQL")
print("=" * 70)
spark.sql(
    """
    SELECT p.id, c.nome, c.cidade, pr.produto, pr.categoria, p.valor_total
    FROM pedidos p
    JOIN clientes c ON p.cliente_id = c.cliente_id
    JOIN produtos pr ON p.produto_id = pr.produto_id
    ORDER BY p.id
    """
).show(truncate=False)

print("=" * 70)
print("CASE WHEN: classificando o valor do pedido em faixas")
print("=" * 70)
spark.sql(
    """
    SELECT
        p.id,
        p.valor_total,
        CASE
            WHEN p.valor_total < 200 THEN 'Baixo'
            WHEN p.valor_total < 1000 THEN 'Médio'
            ELSE 'Alto'
        END AS faixa_valor
    FROM pedidos p
    ORDER BY p.valor_total DESC
    """
).show()

print("=" * 70)
print("Funções de data: YEAR, MONTH e DATEDIFF")
print("=" * 70)
spark.sql(
    """
    SELECT
        id,
        data,
        YEAR(data) AS ano,
        MONTH(data) AS mes,
        DATEDIFF(CURRENT_DATE(), data) AS dias_desde_o_pedido
    FROM pedidos
    ORDER BY data
    """
).show()

print("=" * 70)
print("Funções de texto: UPPER, TRIM e CONCAT")
print("=" * 70)
spark.sql(
    """
    SELECT
        cliente_id,
        UPPER(nome) AS nome_maiusculo,
        CONCAT(TRIM(nome), ' - ', cidade) AS nome_e_cidade
    FROM clientes
    ORDER BY cliente_id
    LIMIT 10
    """
).show(truncate=False)

print("=" * 70)
print("Comparando com a DataFrame API: total por categoria")
print("=" * 70)
print("Via SQL:")
spark.sql(
    """
    SELECT pr.categoria, SUM(p.valor_total) AS total
    FROM pedidos p
    JOIN produtos pr ON p.produto_id = pr.produto_id
    GROUP BY pr.categoria
    ORDER BY total DESC
    """
).show()

print("Via DataFrame API (mesmo resultado):")
pedidos.join(produtos, on="produto_id").groupBy("categoria").sum("valor_total").orderBy(
    "sum(valor_total)", ascending=False
).show()

spark.stop()
