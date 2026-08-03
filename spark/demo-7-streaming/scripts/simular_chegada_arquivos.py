"""
Helper da demo 7 — simula a chegada contínua de um log de aplicação numa
pasta monitorada, para que o Structured Streaming (demo_streaming.py)
tenha o que processar em tempo real.

Divide common/dados/eventos_pequeno.log em pedaços pequenos e vai
escrevendo um arquivo novo na pasta de entrada a cada poucos segundos,
imitando um log real crescendo aos poucos (em vez de um arquivo gigante
chegando de uma vez).

Rodar em um terminal separado, com o streaming (demo_streaming.py) já
rodando em outro:
    docker exec -it spark-course python3 /curso/demo-7-streaming/scripts/simular_chegada_arquivos.py
    docker exec -it spark-course python3 /curso/demo-7-streaming/scripts/simular_chegada_arquivos.py --linhas-por-arquivo 3 --intervalo 3
    docker exec -it spark-course python3 /curso/demo-7-streaming/scripts/simular_chegada_arquivos.py --origem /curso/common/dados/eventos_pedido.log --linhas-por-arquivo 20 --intervalo 4
"""
import argparse
import time
from pathlib import Path

PASTA_ENTRADA = Path("/curso/demo-7-streaming/pasta_entrada")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--origem", default="/curso/common/dados/eventos_pequeno.log",
        help="Log de origem a ser dividido em pedaços (default: eventos_pequeno.log, o das demonstrações)",
    )
    parser.add_argument("--linhas-por-arquivo", type=int, default=4, help="Linhas de log por arquivo (default: 4)")
    parser.add_argument("--intervalo", type=float, default=5.0, help="Segundos entre cada arquivo novo (default: 5)")
    args = parser.parse_args()

    PASTA_ENTRADA.mkdir(parents=True, exist_ok=True)
    linhas = Path(args.origem).read_text(encoding="utf-8").splitlines()

    total_arquivos = (len(linhas) + args.linhas_por_arquivo - 1) // args.linhas_por_arquivo
    print(f"Simulando a chegada de {total_arquivos} arquivos em {PASTA_ENTRADA}, a cada {args.intervalo}s.")

    for i in range(total_arquivos):
        inicio = i * args.linhas_por_arquivo
        pedaco = linhas[inicio: inicio + args.linhas_por_arquivo]
        destino = PASTA_ENTRADA / f"eventos_{i + 1:03d}.log"
        destino.write_text("\n".join(pedaco) + "\n", encoding="utf-8")
        print(f"[{i + 1}/{total_arquivos}] Novo arquivo: {destino.name} ({len(pedaco)} linhas de log)")
        time.sleep(args.intervalo)

    print("Simulação concluída.")


if __name__ == "__main__":
    main()
