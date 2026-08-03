# Demonstração 6 — Lendo 4 formatos diferentes e gravando particionado

Diferente de simplesmente converter um CSV em Parquet, aqui as 4 fontes
já nascem em formatos diferentes de verdade: `clientes.csv`/`pedidos_pequeno.csv`
(CSV), `produtos.json` (JSON) e `eventos_pequeno.log` (log de texto puro,
lido com `spark.read.text` + `regexp_extract`, sem leitor dedicado). O
Spark lê todas com a mesma família de API (`spark.read...`). Depois de
juntar tudo — pedidos + clientes + produtos + um resumo do log por pedido
— grava o resultado final particionado em Parquet.

Corresponde aos slides "Demonstração — Bloco 3" (Dia 2, Bloco 3: Leitura e
escrita de dados), com fontes nativamente heterogêneas em vez de uma
conversão isolada.

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — as
  4 fontes em `common/dados/` (`clientes.csv`, `pedidos_pequeno.csv`,
  `produtos.json`, `eventos_pequeno.log`) precisam existir.

## Passo único — Rodar a demonstração

```bash
docker exec -it spark-course spark-submit /curso/demo-6-leitura-escrita/scripts/demo_formatos.py
```

O resultado fica em
`demo-6-leitura-escrita/saida/pedidos_enriquecidos/`, com uma subpasta por
categoria (efeito do `partitionBy("categoria")`).

## Ideias para explorar em aula

- Comparar como cada formato é lido: `spark.read.csv(...)`,
  `spark.read.json(...)` e `spark.read.text(...)` — a família de API é a
  mesma, só muda o método
- Mostrar `regexp_extract` linha a linha, comparando com o parsing manual
  por regex feito na demo 2 (RDD) — mesma ideia, API diferente
- Abrir a pasta de saída no host e mostrar as subpastas
  `categoria=Informática/`, `categoria=Casa/` etc.
- Perguntar por que alguns pedidos "sumiram" do resultado final (os
  mesmos casos de `cliente_id` nulo e `produto_id` inexistente vistos na
  demo 4 — o join ali continua sendo `inner`)
