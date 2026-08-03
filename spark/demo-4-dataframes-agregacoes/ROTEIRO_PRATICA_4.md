RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 4 — Join de 3 tabelas e agregação por cidade**

---

## Objetivo

Reproduzir o fluxo de limpeza + join + agregação da demonstração, agora
agrupando por **cidade** (em vez de região) em um dataset bem maior, com
mais casos de dado sujo para tratar.

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/pedidos_grande.csv` (300 linhas),
  `common/dados/clientes.csv` e `common/dados/produtos.json` precisam
  existir.

---

## Passo a passo

### Passo 1 — Tratar os nulos

Carregue `pedidos_grande.csv` e remova (`dropna`) os pedidos sem
`cliente_id`. Antes e depois, imprima `count()` para ver quantas linhas
foram descartadas.

### Passo 2 — Corrigir a duplicidade de produtos.json

Antes de fazer o join, confira se `produtos.json` ainda tem o
`produto_id` duplicado (mesma checagem da demonstração:
`groupBy("produto_id").count().filter("count > 1")`) e remova a
duplicidade com `dropDuplicates(["produto_id"])`.

### Passo 3 — Join das 3 tabelas

Junte `pedidos` (já limpo) com `produtos` (já sem duplicidade) e com
`clientes`. **Confira**: o total de linhas depois do join deve ser igual
(ou menor, por causa dos `produto_id` inexistentes) ao total de pedidos
antes do join — nunca maior.

### Passo 4 — Agregação por cidade

Agrupe por `cidade` (não por `regiao`, como na demonstração) e calcule
total vendido, ticket médio e quantidade de pedidos. Ordene pelo total
vendido, do maior para o menor.

---

## O que entregar

- O `count()` antes/depois do `dropna` e antes/depois do join (para
  provar que não houve duplicação de linhas).
- A tabela agregada por cidade, ordenada pelo total vendido.

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- Se o total de linhas depois do join for **maior** que antes, o
  `produto_id` duplicado não foi tratado — volte ao Passo 2.
- Pedidos com `produto_id` que não existe em `produtos.json` somem no
  `inner join` (padrão) — é esperado; se quiser mantê-los mesmo sem os
  dados do produto, use `how="left"` e trate os nulos resultantes.
