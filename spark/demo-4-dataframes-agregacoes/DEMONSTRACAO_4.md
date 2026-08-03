# Demonstração 4 — Joins de verdade, dropna/fillna e agregações pós-join

Este é o bloco em que as 3 fontes da loja se encontram. Antes do join,
trata os pedidos sem `cliente_id` (`dropna`/`fillna`) e mostra, na prática,
por que um join ingênuo com `produtos.json` **duplica linhas** (o
`produto_id=1` está cadastrado duas vezes, de propósito). Corrige com
`dropDuplicates`, faz o join das 3 tabelas, agrega por região (`groupBy` +
`agg` + `orderBy`) e fecha com filtros combinados (`&`/`|`).

Corresponde aos slides "Demonstração — Bloco 1" (Dia 2, Bloco 1: Operações
com DataFrames), incorporando também "Lidando com valores nulos" e
"Introdução a joins" (slides do mesmo bloco).

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/pedidos_pequeno.csv`, `common/dados/clientes.csv`
  e `common/dados/produtos.json` precisam existir.

## Passo único — Rodar a demonstração

```bash
docker exec -it spark-course spark-submit /curso/demo-4-dataframes-agregacoes/scripts/demo_agregacoes.py
```

## Ideias para explorar em aula

- Antes de rodar, perguntar: "quantos pedidos eu tenho?" e "quantos eu
  espero depois do join?" — a resposta errada (mais linhas que antes) é o
  gancho para o problema do produto duplicado
- Mostrar `produtos.groupBy("produto_id").count().filter("count > 1")`
  isoladamente — é assim que se **detecta** esse tipo de problema num
  dataset real, antes mesmo de fazer o join
- **Ponto sutil do Passo 2**: o join de comparação usa `how="left"`, não
  `how="inner"` (o padrão). Com `inner`, o pedido de `produto_id`
  inexistente (visto no Passo 1) desapareceria do resultado ao mesmo
  tempo em que o produto duplicado aumenta a contagem — os dois efeitos
  se cancelam, e a contagem fica igual mesmo com a duplicação
  acontecendo de verdade. Vale rodar com `how="inner"` ao vivo para
  mostrar esse "sinal escondido" antes de explicar por que trocamos para
  `left`
- Comparar `dropna` (perde a linha) com `fillna` (mantém, mas marca com um
  valor "impossível") — discutir quando cada estratégia faz mais sentido
- No `orderBy` final, relacionar com a demo 9: a mesma pergunta poderia
  ser resolvida também via RDD ou SQL
