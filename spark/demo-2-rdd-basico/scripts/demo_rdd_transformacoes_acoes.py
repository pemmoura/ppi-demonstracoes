"""
Demo 2 — Dia 1, Bloco 2: RDDs (básico)

Primeiro contato com RDD: cria um RDD a partir de um log de texto
(eventos_pequeno.log), uma linha por elemento, no formato

    2026-03-02 13:05:00 INFO  pedido_id=2 evento=CRIADO mensagem="Pedido criado com sucesso"

Passos: map (separar cada linha em pedaços com .split()), filter (só os
eventos de erro), count/take, e por fim um join simples com os pedidos
para descobrir o cliente de cada erro.

Rodar dentro do container Spark:
    docker exec -it spark-course spark-submit /curso/demo-2-rdd-basico/scripts/demo_rdd_transformacoes_acoes.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo-2-rdd-basico").getOrCreate()
sc = spark.sparkContext

LOG = "/curso/common/dados/eventos_pequeno.log"
PEDIDOS_CSV = "/curso/common/dados/pedidos_pequeno.csv"

print("=" * 70)
print("Criando um RDD a partir do log (cada linha do arquivo = um elemento)")
print("=" * 70)
rdd_log = sc.textFile(LOG)
print("Total de linhas no log:", rdd_log.count())
print("Uma linha de exemplo:", rdd_log.first())


def parsear_linha(linha):
    """Separa a linha em pedaços por espaço, e pega o que interessa:
    o nível (3º pedaço) e o pedido_id/evento (depois do "=")."""
    partes = linha.split()
    nivel = partes[2]  # "INFO", "WARN" ou "ERROR"
    pedido_id = partes[3].split("=")[1]  # "pedido_id=2" -> "2"
    evento = partes[4].split("=")[1]  # "evento=CRIADO" -> "CRIADO"
    return (pedido_id, nivel, evento)


print("=" * 70)
print("map: separando cada linha em (pedido_id, nivel, evento)")
print("=" * 70)
rdd_eventos = rdd_log.map(parsear_linha)
print("A mesma linha, já separada:", rdd_eventos.first())

print("=" * 70)
print("filter: mantendo só os eventos de nível ERROR")
print("=" * 70)
rdd_erros = rdd_eventos.filter(lambda evento: evento[1] == "ERROR")
print("Total de eventos de erro:", rdd_erros.count())
print("Exemplos (take):", rdd_erros.take(3))

print("=" * 70)
print("join: cruzando cada erro com o cliente do respectivo pedido")
print("=" * 70)
erros_por_pedido = rdd_erros.map(lambda evento: (evento[0], evento[2]))  # (pedido_id, evento)

cabecalho_pedidos = sc.textFile(PEDIDOS_CSV).first()
pedido_para_cliente = (
    sc.textFile(PEDIDOS_CSV)
    .filter(lambda linha: linha != cabecalho_pedidos)
    .map(lambda linha: linha.split(","))
    .map(lambda campos: (campos[0], campos[1]))  # (pedido_id, cliente_id)
)

erros_com_cliente = erros_por_pedido.join(pedido_para_cliente)
for pedido_id, (evento, cliente_id) in erros_com_cliente.collect():
    cliente_txt = cliente_id if cliente_id else "(sem cliente_id cadastrado)"
    print(f"Pedido {pedido_id}: {evento} -- cliente {cliente_txt}")

spark.stop()
