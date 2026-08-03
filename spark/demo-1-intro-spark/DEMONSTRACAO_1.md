# Demonstração 1 — SparkSession e execução preguiçosa (lazy evaluation)

Primeiro contato com o PySpark: criar a `SparkSession`, criar uma
sequência com `spark.range()`, aplicar uma transformação (`withColumn`)
sem chamar nenhuma ação, e só depois chamar `show()` para mostrar
exatamente o momento em que o Spark executa.

Corresponde aos slides "Demonstração — Bloco 1" e "Primeiro contato com o
PySpark" (Dia 1, Bloco 1: Introdução ao Spark). De propósito, não usa RDD
— RDD só é apresentado no próximo bloco (demo 2).

## Pré-requisitos

- A infraestrutura compartilhada precisa estar no ar:

  ```bash
  cd ../common
  docker compose up -d
  ```

- Não depende de nenhum dado gerado por `gerar_dados.py` — usa
  `spark.range()` para criar a sequência direto no script.

## Passo 1 — Só a transformação (sem ação)

```bash
docker exec -it spark-course spark-submit /curso/demo-1-intro-spark/scripts/demo_sparksession_lazy.py
```

O script aplica o `withColumn` e termina sem chamar nenhuma ação. Repare
que nenhuma linha é impressa — o plano foi descrito, mas nada foi
processado.

## Passo 2 — A mesma transformação, agora com a ação

```bash
docker exec -it spark-course spark-submit /curso/demo-1-intro-spark/scripts/demo_sparksession_lazy.py --disparar-acao
```

Agora sim: `show()` dispara o processamento e as 5 linhas aparecem na
tela.

## Ideias para explorar em aula

- Antes de rodar o Passo 2, perguntar à turma: "o que já foi processado
  até agora?" (resposta: nada — só o plano foi montado)
- Relacionar com o slide 11: "transformações descrevem o que fazer, mas não
  executam na hora; ações disparam o processamento"
- Apontar que `appName("demo-1-intro-spark")` é o nome que aparece na
  Spark UI (retomado com mais detalhe na demo 8)
- Reforçar que RDD ainda não entrou em cena — esta demo (e a atividade)
  usam só `spark.range()` e `withColumn()`, API de DataFrame; RDD é
  assunto da demo 2
