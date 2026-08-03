RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 9 — RDD, DataFrame e SQL com join, num dataset maior, com cache()**

---

## Objetivo

Responder a uma nova pergunta usando as três abordagens (RDD, DataFrame,
Spark SQL) em um dataset maior, com um join diferente do usado na
demonstração, aplicando `cache()` e cuidados de memória.

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/pedidos_grande.csv` (300 linhas) e
  `common/dados/clientes.csv` precisam existir.

---

## Passo a passo

Responda à pergunta **"qual cidade teve o maior ticket médio?"** três
vezes, com abordagens diferentes. Diferente da demonstração (que juntou
`pedidos` com `produtos` para achegar à categoria), aqui o join é entre
**pedidos e clientes** (a cidade vem de `clientes.csv`).

### Passo 1 — Via RDD (join manual + soma/contagem)

Construa dois RDDs de pares -- `(cliente_id, valor_total)` a partir de
`pedidos_grande.csv` e `(cliente_id, cidade)` a partir de `clientes.csv`
-- una com `RDD.join()`, e calcule a média por cidade somando os valores
e contando as ocorrências por chave (`reduceByKey`).

### Passo 2 — Via DataFrame

```python
pedidos.join(clientes, on="cliente_id").groupBy("cidade").avg("valor_total").orderBy("avg(valor_total)", ascending=False).first()
```

### Passo 3 — Via Spark SQL

Registre `pedidos` e `clientes` como views e escreva a consulta
equivalente com `AVG` e `ORDER BY ... DESC LIMIT 1`.

### Passo 4 — Aplicar cache() antes de reaproveitar

Se o `join` de pedidos+clientes for usado mais de uma vez no seu código
(por exemplo, para responder também "qual cidade teve mais pedidos?"),
aplique `cache()` nele antes de reutilizá-lo, e libere com `unpersist()`
ao final.

---

## O que entregar

- As três versões do código funcionando, com o resultado de cada uma
  (devem concordar entre si).
- Uma conclusão: qual abordagem pareceu mais clara para esta pergunta.
- Um cuidado citado para evitar erro de memória (OOM) — por exemplo,
  evitar `collect()` em dados muito grandes, ou usar `unpersist()` quando
  o cache não for mais necessário.

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- As três abordagens devem chegar ao **mesmo resultado** — se não
  chegarem, é sinal de que uma delas está calculando a média errado (por
  exemplo, fazendo a média das médias, em vez de soma total / contagem
  total).
- Alguns pedidos não têm `cliente_id` (visto na demo 4) — tanto o
  `RDD.join()` quanto o `join` de DataFrame/SQL simplesmente os deixam de
  fora do resultado; não é preciso tratamento extra.
