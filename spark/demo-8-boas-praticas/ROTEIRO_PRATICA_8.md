RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 8 — Encontrar e corrigir um erro usando a Spark UI**

---

## Objetivo

Aplicar o fluxo de boas práticas da demonstração (amostra pequena → testar
→ rodar tudo) para identificar e corrigir um erro proposital em
`scripts/script_com_erro.py`.

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado.
- Ter revisado a demo 8 (`demo_boas_praticas.py`), que mostra o fluxo de
  teste com amostra pequena.

---

## Passo a passo

### Passo 1 — Rodar o script com erro

```bash
docker exec -it spark-course spark-submit /curso/demo-8-boas-praticas/scripts/script_com_erro.py
```

O script vai falhar. Não se assuste com o tamanho do traceback — vá até
o final da mensagem de erro primeiro.

### Passo 2 — Ler a mensagem de erro

Procure a linha que aponta a causa real do problema (geralmente perto de
`Caused by:` ou na análise do plano lógico) e o nome do seu próprio
arquivo/linha no traceback. Identifique qual coluna foi referenciada
incorretamente.

### Passo 3 — Corrigir o script

Copie `script_com_erro.py` para `script_corrigido.py` e corrija o nome da
coluna incorreto.

### Passo 4 — Rodar a versão corrigida e conferir na Spark UI

```bash
docker exec -it spark-course spark-submit /curso/demo-8-boas-praticas/scripts/script_corrigido.py
```

Acompanhe a execução em **http://localhost:4040** enquanto o script roda.

### Passo 5 — Broadcast join no script corrigido

No `script_corrigido.py`, adicione um join entre o resultado e
`produtos.json` (`spark.read.json(...).dropDuplicates(["produto_id"])`),
usando `F.broadcast(produtos)`. Rode `explain()` no resultado do join e
confirme que o plano mostra `BroadcastHashJoin` (não `SortMergeJoin`).

---

## O que entregar

- `script_corrigido.py` funcionando, já com o join via `F.broadcast()`.
- A mensagem de erro original (print/cópia do traceback).
- Uma explicação curta de qual era o problema e como foi corrigido.
- O `explain()` do join, mostrando `BroadcastHashJoin`.

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- Erros de nome de coluna geram uma mensagem de análise do Spark listando
  as colunas disponíveis — compare com o nome que você usou no código.
- Teste sempre com `.limit(5)` primeiro: se o script tivesse rodado direto
  no dataset completo, o erro apareceria do mesmo jeito, só que depois de
  mais tempo de processamento.
