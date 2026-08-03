# PPI — Processamento Big Data com Spark

Nove demonstrações que acompanham, bloco a bloco, os 3 dias da disciplina
"Processamento Big Data com Spark" (18h). Cada `demo-N-*/` corresponde a um
bloco de aula e evolui o mesmo exemplo-guia — uma loja fictícia com
clientes, produtos e pedidos — do primeiro contato com o PySpark até
streaming, joins entre múltiplas fontes e boas práticas.

## Como este curso é diferente do curso de Data Lakes

O curso de Data Lakes (`../datalakes`) encadeia serviços diferentes a cada
demo (Kafka, MinIO, Postgres, Airflow), por isso cada demo tem sua própria
infraestrutura. Aqui o Spark roda sempre em **modo local**, dentro de um
único container — não há necessidade de subir um serviço novo a cada bloco.
Por isso a infraestrutura é uma só (`common/`), compartilhada por todas as
9 demos, e cada `demo-N-*/` traz apenas os scripts e o roteiro daquele bloco.

## As fontes de dados (múltiplos formatos, de propósito)

O exemplo-guia é uma loja com 4 fontes de dados, cada uma em um formato
diferente — para praticar leitura de CSV, JSON e log de texto, e os joins
entre elas (veja `common/README.md` para os detalhes de cada arquivo):

- **`clientes.csv`** (CSV) — quem comprou
- **`produtos.json`** (JSON) — o catálogo, pequeno o bastante para broadcast join
- **`pedidos_pequeno.csv` / `pedidos_grande.csv`** (CSV) — o fato: cada
  pedido só tem `cliente_id`/`produto_id`, então descobrir o nome do
  cliente/produto **exige join**
- **`eventos_pequeno.log` / `eventos_pedido.log`** (log de texto) — o
  ciclo de vida de cada pedido (criado, pago, enviado, cancelado etc.),
  para praticar parsing de log e streaming

## O pipeline de aprendizado

```
                  ┌─────────────────────────────────────┐
                  │   common/  (Passo 0)                 │
                  │   container "spark-course"           │
                  │   clientes.csv · produtos.json        │
                  │   pedidos_*.csv · eventos_*.log        │
                  └───────────────────┬───────────────────┘
                                      │
  Dia 1 — Primeiros passos           │
  demo-1 SparkSession/lazy → demo-2 RDDs (parse de log + RDD.join) → demo-3 DataFrames (3 fontes, schemas)
                                      │
  Dia 2 — Trabalhando com dados      │
  demo-4 joins + dropna/agregações → demo-5 Spark SQL (join de 3 tabelas) → demo-6 CSV/JSON/log → Parquet
                                      │
  Dia 3 — Streaming e consolidação
  demo-7 streaming de log + stream-static join → demo-8 broadcast x shuffle join → demo-9 RDD x DataFrame x SQL (com join)
```

## Como as demos se conectam

- **Infra compartilhada (`spark/common/`)**: um único container Spark
  (imagem oficial `apache/spark-py`), que monta a pasta `spark/` inteira
  como volume. Sobe uma vez, no início do curso, e fica no ar até o final.
- **`common/scripts/gerar_dados.py`** cria as 4 fontes de dados usadas o
  curso inteiro (clientes, produtos, pedidos e eventos de log), sempre em
  duas escalas: uma pequena para as demonstrações, uma maior para as
  atividades práticas. Rodar uma vez só, depois de subir o container.
- **Cada `demo-N-*/` é independente**: tem seu próprio README (contexto e
  passo a passo da demonstração) e `ROTEIRO_PRATICA_N.md` (o exercício do
  aluno), mas todos executam dentro do mesmo container `spark-course` e
  leem das mesmas fontes de dados em `common/dados/`.
- **A ordem numérica é a ordem de apresentação recomendada** e segue a
  agenda dos slides (3 blocos por dia, 3 dias).

## Ordem de execução

1. `cd common && docker compose up -d` — sobe o container Spark (uma vez só)
2. `docker exec -it spark-course python3 /curso/common/scripts/gerar_dados.py` — gera as fontes de dados usadas no curso inteiro
3. `demo-1-intro-spark` — SparkSession e execução preguiçosa (lazy evaluation)
4. `demo-2-rdd-basico` — RDDs: parsing de log de texto (map/filter/regex) e `RDD.join()`
5. `demo-3-dataframes-intro` — DataFrames: carregar as 3 fontes estruturadas e comparar schemas
6. `demo-4-dataframes-agregacoes` — join de 3 tabelas, `dropna`/`fillna`, filtros combinados, agregações
7. `demo-5-spark-sql` — join de 3 tabelas em SQL, `CASE WHEN`, funções de data/texto
8. `demo-6-leitura-escrita` — ler fontes nativamente diferentes (CSV/JSON/log) e gravar particionado em Parquet
9. `demo-7-streaming` — streaming de um log de aplicação, com stream-static join
10. `demo-8-boas-praticas` — broadcast join x shuffle join, `explain()`, Spark UI
11. `demo-9-cache-comparacao` — cache()/persist(), e a mesma pergunta (com join) via RDD x DataFrame x SQL

Ao final do curso, encerre com `cd common && docker compose down -v`.

## Pré-requisitos gerais

- Docker e Docker Compose
- Porta livre: 4040 (Spark UI, ativa durante a execução de cada job)
- Cada demo lista, no próprio README, o comando exato de `spark-submit`
  usado naquele bloco
