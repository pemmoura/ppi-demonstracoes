# Demonstração 4 — Consultas diretas no Data Lake com DuckDB

Mostra a alternativa ao ETL da demo 3: em vez de mover o dado para um Data
Warehouse, consultamos o parquet de `customers` no bucket `datalake`
**direto de onde ele está**, usando o DuckDB como motor de consulta SQL
sobre o Data Lake.

Corresponde aos slides "Demonstração — Consultas diretas no Data Lake".

## Pré-requisitos

- Docker e Docker Compose instalados
- A infraestrutura compartilhada precisa estar no ar:

  ```bash
  cd ../common
  docker compose up -d
  ```

- A demo 2 precisa ter rodado antes (é ela quem gera staging/processing de
  `customers` no bucket `datalake`).

## Passo 1 — Subir o ambiente

```bash
cd demo-4-datalake-query
docker compose up -d --build
```

## Passo 2 — Consultar ao vivo (shell Python interativo)

Abra um shell Python interativo dentro do container e configure a conexão
com o MinIO:

```bash
docker exec -it dl-duckdb python
```

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("""
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='admin123';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")
con.sql("SELECT * FROM read_parquet('s3://datalake/processing/customers/*.parquet') WHERE last_name ILIKE 'S%'").show()
```

A partir daqui, rode as consultas abaixo (uma de cada vez, no mesmo shell),
todas direto sobre os arquivos parquet no MinIO, sem copiar nada para um
banco:

**1. Schema da camada processing** — mostra que dá pra descobrir a
estrutura do dado só apontando pro parquet, sem catálogo, metastore ou
tabela pré-cadastrada em lugar nenhum.

```python
con.sql("DESCRIBE SELECT * FROM read_parquet('s3://datalake/processing/customers/*.parquet')").show()
```

**2. Listagem completa dos clientes** — prova que o dado já limpo (1 linha
por cliente) pode ser consultado direto, sem carregar em nenhum banco antes.

```python
con.sql("SELECT * FROM read_parquet('s3://datalake/processing/customers/*.parquet') ORDER BY id").show()
```

**3. Comparação staging × processing** — mostra em números o efeito da
deduplicação e da remoção de DELETEs feita pela demo 2: staging tem 1
linha por evento CDC, processing tem 1 linha por cliente.

```python
con.sql("""
    SELECT 'staging (eventos CDC)' AS camada, COUNT(*) AS linhas FROM read_parquet('s3://datalake/staging/customers/*.parquet')
    UNION ALL
    SELECT 'processing (clientes únicos)' AS camada, COUNT(*) AS linhas FROM read_parquet('s3://datalake/processing/customers/*.parquet')
""").show()
```

**4. Clientes por tipo de última operação** — uma agregação ad hoc
(`GROUP BY`), sem precisar exportar nada para um Data Warehouse (contraste
direto com a demo 3).

```python
con.sql("""
    SELECT ultima_operacao, COUNT(*) AS clientes
    FROM read_parquet('s3://datalake/processing/customers/*.parquet')
    GROUP BY ultima_operacao
    ORDER BY clientes DESC
""").show()
```

**5. Domínios de e-mail mais comuns** — usa uma função de string
(`regexp_extract`) para extrair o domínio do e-mail e agrupa por ele; é o
tipo de pergunta que normalmente vira relatório de BI, aqui respondida com
uma consulta ad hoc direto no lake.

```python
con.sql("""
    SELECT regexp_extract(email, '@(.+)$', 1) AS dominio, COUNT(*) AS clientes
    FROM read_parquet('s3://datalake/processing/customers/*.parquet')
    GROUP BY dominio
    ORDER BY clientes DESC
""").show()
```

## Passo 3 — Encerrar

```bash
docker compose down -v
```

## Ideias para explorar em aula

- Rodar `EXPLAIN` antes de uma consulta e comentar que o DuckDB lê só as
  colunas/arquivos necessários do parquet (columnar pruning)
- Comparar o tempo/esforço desta consulta com o ETL da demo 3: aqui não
  existe cópia de dado, schema fixo ou tabela pré-criada
- Perguntar: "quando faz sentido um Data Warehouse (demo 3) em vez de
  consultar direto o lake?" — gancho para discutir performance em escala,
  concorrência de muitos usuários e modelagem dimensional para BI
- Comparar a contagem de staging x processing desta demo com o "RESUMO DO
  QUE MUDOU" impresso pela demo 2 — devem bater
- Na consulta 5 (domínios de e-mail), perguntar: "que outras perguntas de
  negócio dá pra responder assim, sem esperar um ETL rodar?"
