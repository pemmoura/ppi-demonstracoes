# PPI — Demonstrações de Engenharia de Dados

Seis demonstrações que, juntas, formam um pipeline de dados completo e
encadeado — cada uma corresponde a um bloco de aula, mas os dados fluem
de uma para a próxima como se fosse um projeto único.

## O pipeline completo

```
                         ┌─────────────────────────┐
                         │   common/  (Passo 0)     │
                         │   rede "ppi-net" + MinIO │
                         │   buckets: raw, datalake │
                         └────────────┬─────────────┘
                                      │
  demo-1-kafka-connect                │
  Postgres --(Debezium/CDC)--> Kafka --(S3 Sink)--> bucket "raw"
  (dado gerado pelo generate_data.py)  │
                                      │
  demo-2-spark-datalake               ▼
  bucket "raw" --(Spark)--> staging/ --(Spark)--> processing/  (bucket "datalake")
                                      │
                     ┌────────────────┴────────────────┐
                     ▼                                  ▼
  demo-3-data-warehouse              demo-4-datalake-query
  processing --(ETL)--> Postgres     processing --(SQL direto, DuckDB)-->
  (DW, tabela dim_clientes)           consulta sem ETL, sem mover dado

  demo-5-airflow                     demo-6-airflow-dags
  explora a UI/conceitos do Airflow  DAG real: orquestra ingest -> transform
  (DAG ilustrativo, só print())      -> validar -> exportar (demos 2 e 3)
```

## Como as demos se conectam

- **Infra compartilhada (`datalakes/common/`)**: uma rede Docker externa (`ppi-net`)
  e um MinIO com os buckets `raw` e `datalake`. Sobe uma vez, no início do
  curso, e fica no ar durante todas as demos — é o que permite que o dado
  gerado numa demo continue disponível na próxima.
- **Cada `demo-N-*/` é independente**: tem seu próprio `docker-compose.yml`
  e README, pode ser apresentada isoladamente (`cd demo-N-* && docker
  compose up`) e corresponde a um bloco específico dos slides. Nenhuma
  demo duplica a lógica de outra — a demo 6, por exemplo, reaproveita
  literalmente os scripts das demos 2 e 3, só automatizando a execução.
- **A ordem numérica é a ordem de apresentação recomendada**, mas cada
  README lista os pré-requisitos exatos (quais demos precisam ter rodado
  antes) para o instrutor decidir se quer reapresentar do zero ou só
  continuar de onde parou.

## Ordem de execução

1. `cd common && docker compose up -d` — sobe a rede e o MinIO (uma vez só)
2. `demo-1-kafka-connect` — Postgres + Kafka Connect (CDC) gravando no bucket `raw`
   (inclui rodar `generate_data.py`, que gera o volume de eventos que a demo 2 processa)
3. `demo-2-spark-datalake` — Spark processa o `raw` e gera staging/processing no bucket `datalake`
4. `demo-3-data-warehouse` — exporta o processing para um Data Warehouse (Postgres, tabela `dim_clientes`)
5. `demo-4-datalake-query` — consulta o processing direto com DuckDB, sem ETL
6. `demo-5-airflow` — sobe um Airflow e explora a interface/conceitos
7. `demo-6-airflow-dags` — DAG real orquestrando os passos 3 e 4 (ingest → transform → validar → exportar)

Ao final do curso, encerre tudo com `cd common && docker compose down -v`
(isso apaga os dados acumulados no MinIO).

## Pré-requisitos gerais

- Docker e Docker Compose
- Portas livres: 2181, 5432, 5433, 8080, 8083, 9000, 9001, 9092
- Cada demo lista, no próprio README, os pré-requisitos específicos
  (portas e demos anteriores necessárias)
