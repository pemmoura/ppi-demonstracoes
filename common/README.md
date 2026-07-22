# Infraestrutura compartilhada — "Passo 0" do curso

Sobe a rede Docker `ppi-net` e o MinIO usado por todas as demos (buckets
`raw` e `datalake`). Suba isto **uma vez**, no início do curso, e deixe
rodando — cada demo depois se conecta aqui.

## Pré-download das imagens (recomendado antes da aula)

Execute o bloco abaixo com conexão estável para baixar todas as imagens
usadas nas 6 demos de uma vez. Evita espera durante a apresentação.

```bash
docker pull minio/minio:latest
docker pull minio/mc:latest
docker pull confluentinc/cp-zookeeper:7.6.0
docker pull confluentinc/cp-kafka:7.6.0
docker pull confluentinc/cp-kafka-connect-base:7.6.0
docker pull debezium/example-postgres:2.6
docker pull apache/spark-py:v3.4.0
docker pull python:3.11-slim
docker pull postgres:15
docker pull apache/airflow:3.3.0
```

> As imagens das demos 1, 3 e 6 ainda precisam de `docker compose build`
> (Kafka Connect com Debezium, ETL Python e Airflow com PySpark/DuckDB),
> que usa essas imagens base acima. O build só precisa ser feito uma vez
> e pode ser feito com antecedência também:
>
> ```bash
> (cd ../demo-1-kafka-connect && docker compose build)
> (cd ../demo-3-data-warehouse && docker compose build)
> (cd ../demo-6-airflow-dags  && docker compose build)
> ```

## Subir

```bash
cd common
docker compose up -d
```

Aguarde o serviço `ppi-minio-init` encerrar sozinho (ele cria os buckets
`raw` e `datalake` e sai). Confira em **http://localhost:9001**
(usuário: `admin`, senha: `admin123`) que os dois buckets existem.

## Encerrar (só no final do curso, depois da última demo)

```bash
cd common
docker compose down -v
```

> Atenção: `down -v` apaga o volume do MinIO, ou seja, todos os dados
> acumulados pelas demos 1-4 e 6. Só faça isso quando o pipeline
> completo não for mais precisar ser demonstrado.
