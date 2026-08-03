RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 6 — Multi-formato no dataset grande + comparação Parquet x JSON**

---

## Objetivo

Reproduzir a leitura das 4 fontes e o join da demonstração no dataset
grande, e comparar o resultado gravado em Parquet com o mesmo resultado
gravado em JSON (tamanho em disco e velocidade de leitura).

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado —
  `common/dados/pedidos_grande.csv` (300 linhas) e
  `common/dados/eventos_pedido.log` (800 linhas) precisam existir, além de
  `clientes.csv` e `produtos.json`.

---

## Passo a passo

### Passo 1 — Ler as 4 fontes (dataset grande)

Repita a leitura da demonstração, trocando `pedidos_pequeno.csv` por
`pedidos_grande.csv` e `eventos_pequeno.log` por `eventos_pedido.log`.
Lembre-se de tirar a duplicidade de `produtos` com `dropDuplicates`.

### Passo 2 — Resumo do log e join final

Monte o resumo do log por pedido (quantidade de eventos, se teve erro) e
junte com pedidos + clientes + produtos, como na demonstração.

### Passo 3 — Gravar em dois formatos

```python
resultado.write.mode("overwrite").partitionBy("categoria").parquet("/curso/demo-6-leitura-escrita/saida/atividade_parquet")
resultado.write.mode("overwrite").partitionBy("categoria").json("/curso/demo-6-leitura-escrita/saida/atividade_json")
```

### Passo 4 — Comparar

Releia os dois formatos com `spark.read.parquet(...)` e
`spark.read.json(...)`, e compare tamanho em disco (das pastas geradas) e
tempo de leitura (`time.time()` antes/depois de cada `.count()`).

---

## O que entregar

- O resultado final (pedidos + clientes + produtos + resumo do log)
  gravado nos dois formatos.
- Uma comparação de tamanho em disco e tempo de leitura entre Parquet e
  JSON.

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- Com 300 pedidos e 800 linhas de log, a diferença de tamanho/velocidade
  entre os formatos já é bem mais visível do que no dataset pequeno.
- Se o `join` com o resumo do log não achar o `pedido_id`, confira se a
  coluna foi convertida para o mesmo tipo (`int`) dos dois lados.
