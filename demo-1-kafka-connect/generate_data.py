"""
Gerador de eventos — cria uma sequência longa e variada de INSERT/UPDATE/DELETE
na tabela inventory.customers, para alimentar o CDC (demo 1) com volume real
de dado e com problemas de qualidade de propósito (espaços sobrando, caixa
alta/baixa inconsistente). Depois de processados pela demo 2
(staging -> processing), esses problemas ficam visivelmente corrigidos,
mostrando o antes/depois entre as duas camadas.

Por padrão gera 1000 eventos, espalhados ao longo de 5 minutos (300s), para
dar tempo de acompanhar o fluxo chegando no Kafka/MinIO durante uma aula.
Cada evento roda como um comando isolado via `docker exec` no Postgres da
demo 1 (não precisa instalar nada no host além de Docker e Python).

Rodar (com a demo 1 já no ar):
    python generate_data.py
    python generate_data.py --total 200 --duration 60   # mais rápido
    python generate_data.py --total 1000 --duration 0   # sem pausas, o mais rápido possível
    python generate_data.py --container dl-postgres --db inventory
"""
import argparse
import random
import subprocess
import sys
import time

PRIMEIROS_NOMES = [
    "Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela",
    "Heitor", "Isabela", "João", "Karina", "Lucas", "Mariana", "Nicolas",
    "Otávio", "Patrícia", "Rafael", "Sofia", "Thiago", "Vitória",
]
SOBRENOMES = [
    "Silva", "Souza", "Costa", "Lima", "Pereira", "Almeida", "Nogueira",
    "Ribeiro", "Carvalho", "Gomes", "Martins", "Araújo", "Barbosa",
    "Rocha", "Dias", "Teixeira",
]
DOMINIOS = ["email.com", "webmail.com", "correio.net"]


def sujar_texto(texto: str) -> str:
    """Aplica, aleatoriamente, um problema de qualidade de dado ao texto
    (ou nenhum, para o dado também sair limpo às vezes)."""
    estilo = random.choice(["upper", "lower", "espacos", "limpo", "limpo"])
    if estilo == "upper":
        return texto.upper()
    if estilo == "lower":
        return texto.lower()
    if estilo == "espacos":
        meio = texto.replace(" ", "  ") if " " in texto else texto
        return f"  {meio}  "
    return texto


def gerar_email(id_cliente: int, primeiro: str, sobrenome: str) -> str:
    base = f"{primeiro}.{sobrenome}{id_cliente}@{random.choice(DOMINIOS)}"
    base = base.lower().replace("ã", "a").replace("é", "e").replace("í", "i").replace("ó", "o")
    if random.random() < 0.3:
        base = sujar_texto(base)
    return base


def gerar_pessoa(id_cliente: int):
    primeiro = random.choice(PRIMEIROS_NOMES)
    sobrenome = random.choice(SOBRENOMES)
    email = gerar_email(id_cliente, primeiro, sobrenome)
    return sujar_texto(primeiro), sujar_texto(sobrenome), email


def escapar(valor: str) -> str:
    return valor.replace("'", "''")


def montar_eventos(total: int, id_inicial: int = 3000):
    """Gera a sequência de eventos (descrição, SQL), misturando INSERT,
    UPDATE (inclusive sobre o mesmo cliente mais de uma vez) e DELETE,
    mantendo consistência (só atualiza/remove clientes que existem)."""
    ativos = {1001, 1002, 1003, 1004}  # clientes de exemplo já no banco
    proximo_id = id_inicial
    eventos = []

    for i in range(1, total + 1):
        pode_deletar = len(ativos - {1001, 1002, 1003, 1004}) > 5
        pode_atualizar = len(ativos) > 0
        operacao = random.choices(
            ["insert", "update", "delete"],
            weights=[55, 35, 10 if pode_deletar else 0],
        )[0]
        if operacao == "update" and not pode_atualizar:
            operacao = "insert"

        if operacao == "insert":
            id_cliente = proximo_id
            proximo_id += 1
            primeiro, sobrenome, email = gerar_pessoa(id_cliente)
            sql = (
                "INSERT INTO inventory.customers (id, first_name, last_name, email) "
                f"VALUES ({id_cliente}, '{escapar(primeiro)}', '{escapar(sobrenome)}', '{escapar(email)}');"
            )
            descricao = f"[{i}/{total}] INSERT id={id_cliente} ({primeiro.strip()} {sobrenome.strip()})"
            ativos.add(id_cliente)

        elif operacao == "update":
            id_cliente = random.choice(list(ativos))
            primeiro, sobrenome, email = gerar_pessoa(id_cliente)
            sql = (
                "UPDATE inventory.customers "
                f"SET first_name = '{escapar(primeiro)}', last_name = '{escapar(sobrenome)}' "
                f"WHERE id = {id_cliente};"
            )
            descricao = f"[{i}/{total}] UPDATE id={id_cliente} ({primeiro.strip()} {sobrenome.strip()})"

        else:  # delete
            candidatos = list(ativos - {1001, 1002, 1003, 1004})
            id_cliente = random.choice(candidatos)
            sql = f"DELETE FROM inventory.customers WHERE id = {id_cliente};"
            descricao = f"[{i}/{total}] DELETE id={id_cliente}"
            ativos.discard(id_cliente)

        eventos.append((descricao, sql))

    return eventos


def rodar_evento(container: str, db: str, descricao: str, sql: str) -> None:
    print(descricao)
    resultado = subprocess.run(
        ["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-c", sql],
        capture_output=True,
        text=True,
    )
    if resultado.returncode != 0:
        print(resultado.stderr.strip(), file=sys.stderr)
        print(
            "Falhou. Confira se a demo 1 está no ar "
            f"(container '{container}') antes de continuar.",
            file=sys.stderr,
        )
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", default="dl-postgres", help="Nome do container do Postgres")
    parser.add_argument("--db", default="inventory", help="Nome do banco")
    parser.add_argument("--total", type=int, default=1000, help="Total de eventos a gerar (default: 1000)")
    parser.add_argument(
        "--duration", type=float, default=300.0,
        help="Duração alvo em segundos para espalhar todos os eventos (default: 300 = 5 minutos; use 0 para rodar sem pausas)",
    )
    args = parser.parse_args()

    eventos = montar_eventos(args.total)

    print(
        f"Gerando {args.total} eventos em '{args.container}' (banco '{args.db}'), "
        f"ao longo de ~{args.duration:.0f}s.\n"
        "Acompanhe em outro terminal com o console consumer da demo 1 "
        "(veja o Passo 4 do README) para ver os eventos chegando no Kafka.\n"
    )

    inicio = time.time()
    for i, (descricao, sql) in enumerate(eventos, start=1):
        rodar_evento(args.container, args.db, descricao, sql)

        if args.duration > 0:
            decorrido = time.time() - inicio
            restantes = len(eventos) - i
            tempo_restante = max(0.0, args.duration - decorrido)
            pausa = tempo_restante / restantes if restantes > 0 else 0
            time.sleep(pausa)

    total_decorrido = time.time() - inicio
    print("\n" + "-" * 70)
    print(f"Pronto. {args.total} eventos gerados em {total_decorrido:.1f}s.")
    print("-" * 70)


if __name__ == "__main__":
    main()
