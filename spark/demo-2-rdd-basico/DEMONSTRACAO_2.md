# Demonstração 2 — RDDs: map, filter, count/take e um join simples

Primeiro contato com RDD de ponta a ponta, usando a fonte mais "crua" do
curso — um log de aplicação em texto puro (`eventos_pequeno.log`):
`textFile` para ler linha a linha, `map` com `.split()` (sem regex) para
separar os campos, `filter` para isolar os erros, `count`/`take` para
inspecionar o resultado e, por fim, um `RDD.join()` simples para descobrir
o cliente de cada pedido com erro (cruzando com `pedidos_pequeno.csv`).

Corresponde aos slides "Demonstração — Bloco 2" (Dia 1, Bloco 2: RDDs
básico) e aos objetivos do bloco (map, filter, count, take), fechando com
um join entre duas fontes para dar continuidade ao tema de múltiplos
datasets do curso.

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/eventos_pequeno.log` e
  `common/dados/pedidos_pequeno.csv` precisam existir.

## Passo único — Rodar a demonstração

```bash
docker exec -it spark-course spark-submit /curso/demo-2-rdd-basico/scripts/demo_rdd_transformacoes_acoes.py
```

## Ideias para explorar em aula

- Mostrar uma linha do log bruta e a mesma linha já separada em tupla —
  reforça que `map` pode transformar texto livre em dado estruturado
- Explicar `parsear_linha` linha por linha: `.split()` sem argumento
  separa por espaço; `"pedido_id=2".split("=")` separa em `["pedido_id", "2"]`,
  e `[1]` pega só o valor
- Perguntar: "por que `.split()` funciona aqui sem regex?" — porque o
  formato do log é sempre o mesmo, campo por campo, na mesma ordem
- No `RDD.join()`, apontar que o resultado tem a forma
  `(chave, (valor_do_rdd_1, valor_do_rdd_2))` — é assim que o Spark
  representa o "casamento" das duas fontes por `pedido_id`
- Relacionar com o slide "Introdução a joins" (Dia 2, Bloco 1): aqui o
  join é entre RDDs; a partir da demo 4, os mesmos dados aparecem como
  DataFrames, com uma sintaxe mais simples para o mesmo conceito
