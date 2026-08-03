# Demonstração 8 — Amostra pequena, e broadcast join x shuffle join

Duas partes: (1) o fluxo recomendado antes de rodar um pipeline inteiro —
testar a lógica em uma amostra pequena (`.limit(5)`), conferir o plano com
`explain()`, e só então rodar no dataset completo; (2) uma comparação
prática entre **shuffle join** (`SortMergeJoin`, forçado desligando o
broadcast automático) e **broadcast join** (`BroadcastHashJoin`, via
`F.broadcast()`), juntando `pedidos_grande.csv` com o pequeno
`produtos.json`.

Corresponde aos slides "Demonstração — Bloco 2" (Dia 3, Bloco 2: Revisão e
boas práticas), incorporando também "Shuffle e particionamento" (do
mesmo dia, Bloco 3).

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/pedidos_grande.csv` e `common/dados/produtos.json`
  precisam existir.

## Passo único — Rodar a demonstração

```bash
docker exec -it spark-course spark-submit /curso/demo-8-boas-praticas/scripts/demo_boas_praticas.py
```

Depois de rodar os jobs, o script fica esperando parado (sem consumir CPU)
até você apertar **Ctrl+C** — é a deixa para abrir **http://localhost:4040**
em outra aba e explorar jobs, stages e tasks com calma antes de encerrar
(a UI fecha assim que o script termina de vez).

## Ideias para explorar em aula

- Na aba "Jobs" da Spark UI, relacionar cada `.count()`/`.show()` do
  script com um job disparado — reforça que ações (não transformações)
  criam jobs
- Abrir uma "Stage" e mostrar as tasks rodando em paralelo, uma por
  partição
- Comparar os dois `explain()` lado a lado: o shuffle join mostra
  `Exchange`/`SortMergeJoin` (dado embaralhado pela rede); o broadcast
  join mostra `BroadcastExchange`/`BroadcastHashJoin` (produtos.json
  copiado inteiro para cada executor, sem embaralhar os pedidos)
- Perguntar: "por que faz sentido fazer broadcast de `produtos.json` mas
  não de `pedidos_grande.csv`?" — o tamanho da tabela é o que importa
  (`spark.sql.autoBroadcastJoinThreshold`, 10MB por padrão)

## Sobre o `script_com_erro.py`

Esse script (nesta mesma pasta) tem um erro proposital de nome de coluna
— é o material usado na atividade prática (`ROTEIRO_PRATICA_8.md`), não
uma demonstração para rodar em aula sem contexto.
