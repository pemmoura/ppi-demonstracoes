RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 5 — Join de 3 tabelas + CASE WHEN num dataset maior**

---

## Objetivo

Escrever consultas SQL próprias (join de 3 tabelas, `CASE WHEN` e função
de data) sobre o dataset grande, e comparar uma delas com a abordagem via
DataFrame API.

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/pedidos_grande.csv` (300 linhas),
  `common/dados/clientes.csv` e `common/dados/produtos.json` precisam
  existir.

---

## Passo a passo

### Passo 1 — Registrar as views

Carregue as 3 fontes (lembre-se de tirar a duplicidade de `produtos` com
`dropDuplicates(["produto_id"])` antes de registrar a view) e registre
`pedidos`, `clientes` e `produtos`.

### Passo 2 — Join de 3 tabelas com GROUP BY

Escreva uma consulta que junte as 3 tabelas e calcule o **total vendido
por segmento de cliente** (`segmento`), em vez de por categoria de
produto (usado na demonstração).

### Passo 3 — CASE WHEN diferente da demonstração

Crie uma faixa baseada na **quantidade** do pedido (ex.: `"Unitário"` para
`quantidade = 1`, `"Pequeno lote"` para até 3, `"Lote grande"` acima
disso) — em vez da faixa por valor usada na demonstração.

### Passo 4 — Comparar com DataFrame API

Escolha uma das consultas acima e resolva também com a DataFrame API,
conferindo que o resultado é o mesmo.

---

## O que entregar

- As consultas SQL executadas com resultado exibido.
- A versão em DataFrame API de uma delas, com o resultado comparado.

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- Se esquecer o `dropDuplicates(["produto_id"])` antes de registrar a
  view `produtos`, o total vendido por segmento vai vir inflado (mesmo
  problema da demo 4, agora em SQL).
- `CASE WHEN` sempre precisa de `END` no final — é um erro comum de
  sintaxe esquecer.
