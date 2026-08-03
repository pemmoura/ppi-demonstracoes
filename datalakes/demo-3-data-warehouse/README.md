# Demonstração 3 — Exportando para um Data Warehouse

Mostra a diferença entre Data Lake e Data Warehouse na prática: lê a
camada processing de `customers` (gerada pela demo 2, no bucket `datalake`
do MinIO) e a exporta para um Postgres organizado como Data Warehouse —
uma tabela `dim_clientes` pronta para ser consultada com SQL comum, por
qualquer ferramenta de BI, sem precisar de Spark ou saber ler parquet.

Corresponde aos slides "Demonstração — Exportando para um Data Warehouse".

## Pré-requisitos

- Docker e Docker Compose instalados
- Porta livre: 5433
- A infraestrutura compartilhada precisa estar no ar:

  ```bash
  cd ../common
  docker compose up -d
  ```

- A demo 2 precisa ter rodado antes (é ela quem gera `processing/customers`
  no bucket `datalake`).

## Passo 1 — Subir o ambiente

```bash
cd demo-3-data-warehouse
docker compose up -d --build
```

Isso sobe o Postgres do Data Warehouse (`dw-postgres`) e um container
Python (`dw-etl`) com as bibliotecas necessárias para ler parquet do MinIO
e escrever no Postgres.

## Passo 2 — Rodar o ETL: processing → Data Warehouse

```bash
docker exec -it dw-etl python export_to_dw.py
```

O script imprime a camada processing (lida direto do parquet no MinIO) e,
em seguida, a tabela `dim_clientes` gerada a partir dela, já com o campo
`atualizado_em` convertido de epoch (`ts_ms`) para timestamp legível.

## Passo 3 — Consultar o Data Warehouse

```bash
docker exec -it dw-postgres psql -U dw -d dw -c "\dt"
```

Rode uma consulta analítica típica de Data Warehouse (quantos clientes
foram criados/atualizados por mês):

```bash
docker exec -it dw-postgres psql -U dw -d dw -c "
SELECT date_trunc('month', atualizado_em) AS mes, COUNT(*) AS clientes
FROM dim_clientes
GROUP BY mes
ORDER BY mes;
"
```

## Passo 4 — Encerrar

```bash
docker compose down -v
```

## Ideias para explorar em aula

- Comparar como se consulta o processing no Data Lake (Spark/DuckDB,
  precisa saber ler parquet) com o Data Warehouse (SQL comum, qualquer
  ferramenta de BI já conecta em um Postgres)
- Rodar a mesma consulta de agregação primeiro "na unha" sobre o processing
  (com pandas) e depois via SQL no Data Warehouse, comparando a experiência
- Perguntar: "por que não consultar direto o Data Lake, sem esse ETL?" —
  gancho para a demo 4, que faz exatamente isso
