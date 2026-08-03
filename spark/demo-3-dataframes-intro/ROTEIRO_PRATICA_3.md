RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 3 — Explorando as 3 fontes com select/filter/withColumn**

---

## Objetivo

Carregar as 3 fontes estruturadas da loja (incluindo o dataset de pedidos
maior) e aplicar seleção, filtro e criação de coluna diferentes dos usados
na demonstração.

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/clientes.csv`, `common/dados/produtos.json` e
  `common/dados/pedidos_grande.csv` (300 linhas) precisam existir.

---

## Passo a passo

### Passo 1 — clientes.csv

Carregue `clientes.csv` e filtre só os clientes do segmento `"VIP"`.
Selecione apenas as colunas `nome` e `cidade` do resultado.

### Passo 2 — produtos.json

Carregue `produtos.json`, rode `printSchema()` e filtre só os produtos
com `preco_base` acima de R$200.

### Passo 3 — pedidos_grande.csv

Carregue `pedidos_grande.csv`, rode `describe()` para ver as estatísticas
básicas de `valor_total` e `quantidade`, e crie uma coluna nova com
`withColumn` — por exemplo, um desconto fixo de 10% sobre `valor_total`.

---

## O que entregar

- Os 3 DataFrames carregados, com os filtros e a coluna nova pedidos
  exibidos via `show()`.
- Uma frase respondendo: "com esses 3 DataFrames separados, dá para saber
  o nome do produto de um pedido específico? Por quê?"

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- `produtos.json` tem uma linha duplicada de propósito (`produto_id=1`
  aparece duas vezes) — não precisa tratar isso ainda, só repare que ela
  existe; ela vira assunto na atividade 4.
- Ao filtrar por texto (ex.: `segmento == "VIP"`), atenção a maiúsculas —
  o valor precisa bater exatamente com o que está no CSV.
