# Demonstração 9 — RDD x DataFrame x SQL (com join), e cache()

Responde à mesma pergunta ("qual categoria vendeu mais?") usando as três
abordagens do curso — RDD, DataFrame e Spark SQL. Como a categoria só
existe em `produtos.json`, as três precisam fazer o mesmo join com
`pedidos_pequeno.csv` para chegar à resposta — inclusive a versão em RDD,
com `RDD.join()` manual. Depois, mede o tempo de reaproveitar um
DataFrame enriquecido (pedidos + clientes + produtos) em 3 agregações
diferentes, com e sem `cache()`.

Corresponde aos slides "Demonstração — Bloco 3" (Dia 3, Bloco 3: Boas
práticas e comparação de abordagens).

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/pedidos_pequeno.csv`, `common/dados/pedidos_grande.csv`,
  `common/dados/clientes.csv` e `common/dados/produtos.json` precisam
  existir.

## Passo único — Rodar a demonstração

```bash
docker exec -it spark-course spark-submit /curso/demo-9-cache-comparacao/scripts/demo_cache_comparacao.py
```

## Ideias para explorar em aula

- Comparar as 3 versões de código lado a lado — a versão RDD precisa de
  bem mais linhas para fazer o mesmo join que `DataFrame`/`SQL` resolvem
  em uma chamada
- Mostrar que `RDD.join()` também é inner join por padrão — o pedido com
  `produto_id` inexistente (visto na demo 4) some da resposta sem erro,
  igual acontece com DataFrame/SQL
- Com 300 pedidos, o ganho do `cache()` pode ser pequeno; deixar claro que
  o benefício cresce com o tamanho do dado e o número de reaproveitamentos
- Fechar relacionando com a demo 8: o join usado aqui (`pedidos.join(produtos)`)
  é pequeno o bastante para o Spark decidir broadcast sozinho
