RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 1 — SparkSession e execução preguiçosa**

---

## Objetivo

Reproduzir a criação da `SparkSession` e comprovar a execução preguiçosa
(lazy evaluation) com um conjunto de dados maior que o da demonstração.

---

## Antes de começar

- Ambiente de contêineres Docker da demo 1 em execução (`common/` no ar).
- Ferramentas: Docker — local, GitHub Codespaces ou Google Cloud Shell
  (veja `common/README.md` para os detalhes de cada ambiente).

---

## Passo a passo

> Esta atividade ainda **não usa RDD** — RDD só é apresentado na próxima
> aula (demo 2). Fique só na API de DataFrame (`spark.range()`,
> `withColumn`, `select`, `show`/`count`), do mesmo jeito que a
> demonstração.

### Passo 1 — Criar uma sequência maior

Usando a `SparkSession` do mesmo jeito que na demonstração, crie uma
sequência com **20 linhas** (`spark.range(20)`), representando 20
produtos da loja.

### Passo 2 — Aplicar uma transformação própria

Aplique uma transformação diferente da usada na demonstração — por
exemplo, criar uma coluna `preco` calculada a partir do `id`
(`withColumn("preco", (df.id + 1) * 10)`), ou uma coluna de texto
diferente da usada na demo. Não chame nenhuma ação ainda.

### Passo 3 — Disparar a execução e identificar o momento exato

Chame `show()` ou `count()` e identifique, no seu código, em que linha o
Spark realmente processou os dados (a linha da ação, não a da
transformação).

---

## O que entregar

- Print do código e da saída.
- Uma frase indicando o momento exato da execução (evidência de lazy
  evaluation) — por exemplo: "o Spark só processou as 20 linhas quando
  chamei `.show()` na linha X; até ali, `.withColumn()` só tinha descrito
  o plano."

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- Se o `.withColumn()` "parecer" já ter rodado (porque não deu erro),
  lembre-se: isso só significa que o plano foi montado com sucesso, não
  que os dados foram processados.
- `show()`/`count()` são ações — com 20 linhas não há problema nenhum,
  mas `collect()` traz tudo para o Driver, o tipo de chamada a evitar com
  datasets grandes (voltamos a esse ponto na demo 9).
