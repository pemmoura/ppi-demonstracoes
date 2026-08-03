RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 2 — RDDs: log maior, outro nível de evento, outro join**

---

## Objetivo

Reproduzir o fluxo de map/filter/count/take/join da demonstração em um
log bem maior, filtrando um nível de evento diferente e cruzando o
resultado com os **produtos**, não com os clientes.

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/eventos_pedido.log` (800 linhas) e
  `common/dados/pedidos_grande.csv` (300 linhas) precisam existir.

---

## Passo a passo

### Passo 1 — Ler e separar o log grande

Reaproveite a função `parsear_linha` da demonstração (o mesmo
`.split()`, sem regex), agora sobre `common/dados/eventos_pedido.log`.

### Passo 2 — Filtrar um nível diferente do usado na demonstração

A demonstração filtrou o nível `ERROR`. Nesta atividade, filtre o nível
**`WARN`** (eventos `ESTOQUE_INSUFICIENTE` e `CANCELADO`). Use `count()`
para saber quantos warnings existem no total, e `take(5)` para ver alguns
exemplos.

### Passo 3 — Join com produtos, não com clientes

Diferente da demonstração (que cruzou com `cliente_id`), aqui o objetivo é
descobrir **qual produto está por trás de cada warning**:

1. Carregue `pedidos_grande.csv` como RDD e construa os pares
   `(pedido_id, produto_id)` (mesma ideia usada para `cliente_id` na
   demonstração).
2. Faça o `join()` entre os warnings (`pedido_id → evento`) e os pedidos
   (`pedido_id → produto_id`).
3. Use `take(10)` ou `collect()` para listar os resultados.

---

## O que entregar

- Contagem total de warnings (`count()`).
- A lista de warnings já cruzada com o `produto_id` de cada pedido.
- Uma frase indicando quais chamadas do seu código foram transformação
  (`map`, `filter`, `join`) e quais foram ação (`count`, `take`,
  `collect`).

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- Alguns `pedido_id` do log não existem em `pedidos_grande.csv` (de
  propósito) — o `join()` de RDD simplesmente ignora essas chaves sem
  correspondência (é um "inner join"), então não é preciso tratar nada
  manualmente, mas vale mencionar isso na entrega.
- `.split()` só funciona bem porque o log tem sempre o mesmo formato,
  campo por campo, na mesma ordem — se um dia o formato do log mudar,
  esse parsing simples deixaria de funcionar (é uma limitação real de
  fazer parsing "na mão").
