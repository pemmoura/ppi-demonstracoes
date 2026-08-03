"""
Gerador dos datasets usados no curso inteiro. Modela uma loja fictícia com
4 fontes de dados, de propósito em formatos diferentes (para praticar
leitura de CSV, JSON e log de texto, e joins entre elas):

    clientes.csv          (CSV)  -> dimensão cliente
    produtos.json         (JSON, um objeto por linha) -> dimensão produto (pequena, ótima para broadcast join)
    pedidos_pequeno.csv   (CSV)  -> fato: 10 pedidos, para demonstrações
    pedidos_grande.csv    (CSV)  -> fato: 300 pedidos, para atividades práticas
    eventos_pequeno.log   (texto, log de aplicação) -> eventos dos pedidos pequenos, para demonstrações
    eventos_pedido.log    (texto, log de aplicação) -> eventos dos pedidos grandes, para atividades práticas

Os pedidos referenciam clientes (cliente_id) e produtos (produto_id) --
por isso, para saber o nome do cliente/produto de um pedido, é preciso
fazer join. Sujeiras propositais, para praticar tratamento de dado real:

    - alguns pedidos têm cliente_id vazio (nulo)                  -> dropna()/fillna()
    - alguns pedidos referenciam um produto_id que não existe     -> inner join x left join
    - um produto_id aparece duas vezes em produtos.json           -> "join mal feito duplica linha"
    - alguns eventos de log referenciam um pedido_id que não existe -> mesmo problema, agora em streaming

Rodar uma vez, com o container "spark-course" já no ar:
    docker exec -it spark-course python3 /curso/common/scripts/gerar_dados.py
"""
import argparse
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

PRIMEIROS_NOMES = [
    "Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela",
    "Heitor", "Isabela", "João", "Karina", "Lucas", "Mariana", "Nicolas",
    "Otávio", "Patrícia", "Rafael", "Sofia", "Thiago", "Vitória",
]
SOBRENOMES = [
    "Silva", "Souza", "Costa", "Lima", "Pereira", "Almeida", "Nogueira",
    "Ribeiro", "Carvalho", "Gomes", "Martins", "Araújo", "Barbosa",
]
SEGMENTOS = ["Novo", "Recorrente", "VIP"]

CIDADES_REGIAO = {
    "São Paulo": "Sudeste", "Rio de Janeiro": "Sudeste", "Belo Horizonte": "Sudeste",
    "Curitiba": "Sul", "Porto Alegre": "Sul",
    "Salvador": "Nordeste", "Recife": "Nordeste", "Fortaleza": "Nordeste",
    "Manaus": "Norte", "Brasília": "Centro-Oeste",
}

# categoria -> [(nome do produto, faixa de preço base)]
CATALOGO_PRODUTOS = {
    "Informática": [
        ("Mouse", (39.9, 89.9)), ("Teclado", (59.9, 199.9)), ("Monitor", (699.9, 1899.9)),
        ("Notebook", (2499.9, 5999.9)), ("Fone de ouvido", (49.9, 349.9)),
        ("Webcam", (99.9, 299.9)), ("SSD externo", (249.9, 699.9)),
    ],
    "Casa": [
        ("Panela", (89.9, 349.9)), ("Liquidificador", (129.9, 399.9)),
        ("Jogo de toalhas", (69.9, 199.9)), ("Luminária", (59.9, 249.9)),
        ("Cortina", (79.9, 229.9)),
    ],
    "Livros": [
        ("Romance", (29.9, 59.9)), ("Livro técnico", (89.9, 199.9)),
        ("Quadrinho", (24.9, 49.9)), ("Biografia", (39.9, 79.9)),
    ],
    "Esporte": [
        ("Tênis de corrida", (199.9, 599.9)), ("Bola de futebol", (69.9, 179.9)),
        ("Squeeze", (29.9, 69.9)), ("Corda de pular", (19.9, 49.9)),
    ],
    "Brinquedos": [
        ("Quebra-cabeça", (39.9, 99.9)), ("Boneco de ação", (49.9, 149.9)),
        ("Jogo de tabuleiro", (79.9, 199.9)),
    ],
}
FORNECEDORES = ["TechSupply", "CasaBoa Distribuidora", "Livro Fácil", "Esporte Total", "BrinquedoMax"]

PRODUTO_ID_INEXISTENTE = 9999  # usado de propósito em alguns pedidos/eventos, para simular referência quebrada
DATA_INICIAL = date(2026, 1, 1)


def gerar_pessoa():
    return random.choice(PRIMEIROS_NOMES), random.choice(SOBRENOMES)


def gerar_clientes(total: int) -> list[dict]:
    clientes = []
    for cliente_id in range(1, total + 1):
        primeiro, sobrenome = gerar_pessoa()
        cidade = random.choice(list(CIDADES_REGIAO.keys()))
        clientes.append({
            "cliente_id": cliente_id,
            "nome": f"{primeiro} {sobrenome}",
            "cidade": cidade,
            "regiao": CIDADES_REGIAO[cidade],
            "segmento": random.choice(SEGMENTOS),
        })
    return clientes


def gerar_produtos() -> list[dict]:
    produtos = []
    produto_id = 1
    for categoria, itens in CATALOGO_PRODUTOS.items():
        for nome, (preco_min, preco_max) in itens:
            produtos.append({
                "produto_id": produto_id,
                "produto": nome,
                "categoria": categoria,
                "preco_base": round(random.uniform(preco_min, preco_max), 2),
                "fornecedor": random.choice(FORNECEDORES),
            })
            produto_id += 1

    # Sujeira proposital: o produto_id=1 (Mouse) foi recadastrado por engano
    # com um fornecedor e preço diferentes -- um join ingênuo por produto_id
    # duplica a linha de qualquer pedido desse produto.
    original = produtos[0]
    produtos.append({
        "produto_id": original["produto_id"],
        "produto": original["produto"],
        "categoria": original["categoria"],
        "preco_base": round(original["preco_base"] * 1.15, 2),
        "fornecedor": "Distribuidora Nacional (cadastro duplicado)",
    })
    return produtos


def gerar_pedidos(
    total: int, id_inicial: int, total_clientes: int, produtos: list[dict],
    garantir_casos_especiais: bool = False,
) -> list[dict]:
    """Gera os pedidos. Com poucas linhas (o dataset "pequeno" das
    demonstrações), as sujeiras propositais (~5% de chance cada) podem não
    aparecer nenhuma vez por puro acaso -- por isso, quando
    garantir_casos_especiais=True, as 3 primeiras linhas são forçadas a
    cobrir cada caso (cliente_id nulo, produto_id duplicado, produto_id
    inexistente), garantindo que toda demonstração seja reproduzível."""
    produtos_validos = [p["produto_id"] for p in produtos]
    preco_por_produto = {p["produto_id"]: p["preco_base"] for p in produtos}

    pedidos = []
    for i in range(total):
        pedido_id = id_inicial + i

        # ~5% dos pedidos ficam sem cliente_id (campo vazio -> nulo no CSV)
        cliente_id = "" if random.random() < 0.05 else random.randint(1, total_clientes)

        # ~5% dos pedidos referenciam um produto que não existe em produtos.json
        if random.random() < 0.05:
            produto_id = PRODUTO_ID_INEXISTENTE
            preco = round(random.uniform(19.9, 999.9), 2)
        else:
            produto_id = random.choice(produtos_validos)
            preco = preco_por_produto[produto_id]

        quantidade = random.randint(1, 5)
        valor_total = round(preco * quantidade * random.uniform(0.95, 1.05), 2)
        data_pedido = DATA_INICIAL + timedelta(days=random.randint(0, 180))

        pedidos.append({
            "id": pedido_id,
            "cliente_id": cliente_id,
            "produto_id": produto_id,
            "quantidade": quantidade,
            "valor_total": valor_total,
            "data": data_pedido.isoformat(),
        })

    if garantir_casos_especiais and len(pedidos) >= 3:
        pedidos[0]["cliente_id"] = ""  # garante 1 pedido sem cliente_id

        produto_duplicado_id = produtos[0]["produto_id"]  # produto_id=1 (Mouse), cadastrado 2x
        pedidos[1]["produto_id"] = produto_duplicado_id
        preco = preco_por_produto[produto_duplicado_id]
        pedidos[1]["valor_total"] = round(preco * pedidos[1]["quantidade"], 2)

        pedidos[2]["produto_id"] = PRODUTO_ID_INEXISTENTE  # garante 1 produto_id inexistente

    return pedidos


# evento -> (nível de log, lista de mensagens possíveis)
EVENTOS_POSSIVEIS = {
    "CRIADO": ("INFO", ["Pedido criado com sucesso"]),
    "PAGAMENTO_APROVADO": ("INFO", ["Pagamento aprovado"]),
    "PAGAMENTO_RECUSADO": ("ERROR", [
        "Cartao recusado pela operadora", "Saldo insuficiente",
        "Timeout ao processar pagamento",
    ]),
    "ESTOQUE_INSUFICIENTE": ("WARN", ["Estoque insuficiente para o item"]),
    "ENVIADO": ("INFO", ["Pedido enviado a transportadora"]),
    "ENTREGUE": ("INFO", ["Pedido entregue ao cliente"]),
    "CANCELADO": ("WARN", ["Pedido cancelado apos falha no pagamento", "Pedido cancelado por falta de estoque"]),
}


def montar_sequencia_evento(pedido_id: int, data_pedido: str) -> list[tuple[datetime, str, str, str, str]]:
    """Monta a sequência de eventos de um pedido (todas as linhas de log
    que ele gera), como tuplas (timestamp, nivel, pedido_id, evento, mensagem)."""
    inicio = datetime.fromisoformat(data_pedido) + timedelta(
        hours=random.randint(8, 20), minutes=random.randint(0, 59)
    )
    caminho = random.choices(
        ["sucesso", "pagamento_recusado", "sem_estoque"],
        weights=[70, 20, 10],
    )[0]

    if caminho == "sucesso":
        eventos = ["CRIADO", "PAGAMENTO_APROVADO", "ENVIADO", "ENTREGUE"]
    elif caminho == "pagamento_recusado":
        eventos = ["CRIADO", "PAGAMENTO_RECUSADO", "CANCELADO"]
    else:
        eventos = ["CRIADO", "ESTOQUE_INSUFICIENTE", "CANCELADO"]

    linhas = []
    momento = inicio
    for evento in eventos:
        nivel, mensagens = EVENTOS_POSSIVEIS[evento]
        linhas.append((momento, nivel, str(pedido_id), evento, random.choice(mensagens)))
        momento += timedelta(minutes=random.randint(2, 45))
    return linhas


def gerar_eventos_log(pedidos: list[dict], total_linhas: int) -> list[str]:
    linhas_geradas = []
    while len(linhas_geradas) < total_linhas:
        pedido = random.choice(pedidos)
        # ~2% das sequências referenciam um pedido_id que não existe de verdade
        pedido_id = pedido["id"] if random.random() > 0.02 else pedido["id"] + 100000
        linhas_geradas.extend(montar_sequencia_evento(pedido_id, pedido["data"]))

    linhas_geradas.sort(key=lambda linha: linha[0])
    linhas_geradas = linhas_geradas[:total_linhas]

    linhas_formatadas = []
    for momento, nivel, pedido_id, evento, mensagem in linhas_geradas:
        timestamp = momento.strftime("%Y-%m-%d %H:%M:%S")
        linhas_formatadas.append(
            f'{timestamp} {nivel:<5} pedido_id={pedido_id} evento={evento} mensagem="{mensagem}"'
        )
    return linhas_formatadas


def escrever_csv(caminho: Path, linhas: list[dict]) -> None:
    import csv

    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=list(linhas[0].keys()))
        escritor.writeheader()
        escritor.writerows(linhas)
    print(f"Gerado {caminho} ({len(linhas)} linhas)")


def escrever_json_lines(caminho: Path, linhas: list[dict]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as arquivo:
        for linha in linhas:
            arquivo.write(json.dumps(linha, ensure_ascii=False) + "\n")
    print(f"Gerado {caminho} ({len(linhas)} linhas)")


def escrever_log(caminho: Path, linhas: list[str]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as arquivo:
        arquivo.write("\n".join(linhas) + "\n")
    print(f"Gerado {caminho} ({len(linhas)} linhas)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--total-clientes", type=int, default=40, help="Linhas de clientes.csv (default: 40)")
    parser.add_argument("--pequeno-total", type=int, default=10, help="Pedidos das demonstrações (default: 10)")
    parser.add_argument("--grande-total", type=int, default=300, help="Pedidos das atividades práticas (default: 300)")
    parser.add_argument("--eventos-pequeno-total", type=int, default=30, help="Linhas de log das demonstrações (default: 30)")
    parser.add_argument("--eventos-grande-total", type=int, default=800, help="Linhas de log das atividades práticas (default: 800)")
    parser.add_argument("--pasta", default="/curso/common/dados", help="Pasta de saída (default: /curso/common/dados)")
    args = parser.parse_args()

    random.seed(42)  # resultado reprodutível entre turmas/execuções
    pasta = Path(args.pasta)

    clientes = gerar_clientes(args.total_clientes)
    escrever_csv(pasta / "clientes.csv", clientes)

    produtos = gerar_produtos()
    escrever_json_lines(pasta / "produtos.json", produtos)

    pedidos_pequeno = gerar_pedidos(
        args.pequeno_total, 1, args.total_clientes, produtos, garantir_casos_especiais=True
    )
    escrever_csv(pasta / "pedidos_pequeno.csv", pedidos_pequeno)

    pedidos_grande = gerar_pedidos(args.grande_total, 1, args.total_clientes, produtos)
    escrever_csv(pasta / "pedidos_grande.csv", pedidos_grande)

    eventos_pequeno = gerar_eventos_log(pedidos_pequeno, args.eventos_pequeno_total)
    escrever_log(pasta / "eventos_pequeno.log", eventos_pequeno)

    eventos_grande = gerar_eventos_log(pedidos_grande, args.eventos_grande_total)
    escrever_log(pasta / "eventos_pedido.log", eventos_grande)


if __name__ == "__main__":
    main()
