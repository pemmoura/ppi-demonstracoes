# Demonstração 3 — DataFrames: 3 fontes, 3 schemas diferentes

Carrega as 3 fontes estruturadas da loja em DataFrames separados —
`clientes.csv` (CSV), `produtos.json` (JSON) e `pedidos_pequeno.csv`
(CSV) — e mostra `printSchema()`, `select()`, `filter()`, `describe()` e
`withColumn()` em cada uma. Ainda **sem join** — isso é o assunto da
demo 4 — mas já prepara o terreno: os pedidos só têm `cliente_id` e
`produto_id`, não o nome de ninguém.

Corresponde aos slides "Demonstração — Bloco 3" (Dia 1, Bloco 3:
Introdução aos DataFrames), com 3 fontes reais em vez de uma lista
digitada no código.

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/clientes.csv`, `common/dados/produtos.json` e
  `common/dados/pedidos_pequeno.csv` precisam existir.

## Passo único — Rodar a demonstração

```bash
docker exec -it spark-course spark-submit /curso/demo-3-dataframes-intro/scripts/demo_dataframes_intro.py
```

## Ideias para explorar em aula

- Comparar os 3 `printSchema()` — mesmo vindo de formatos diferentes
  (CSV, JSON), o Spark representa tudo como DataFrame com colunas e
  tipos, de forma uniforme
- Perguntar: "como eu descubro o nome do cliente de um pedido, só com o
  que está aqui?" — a resposta ("não dá, falta juntar com `clientes.csv`")
  é o gancho direto para a demo 4
- Mostrar que `produtos.json` tem uma linha "estranha" (o `produto_id=1`
  aparece duas vezes) — sem chamar atenção demais ainda, é a pista que
  vira problema real quando fizermos o join
