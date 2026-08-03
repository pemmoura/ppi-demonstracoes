RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Engenharia de Dados e Data Lakes**
**Atividade Prática 4 — Consultas próprias no Data Lake**

---

## Objetivo

Utilizar o DuckDB para executar consultas SQL diretamente sobre os arquivos Parquet da camada Processing do Data Lake, extraindo informações sem a necessidade de um banco relacional tradicional.

---

## Antes de começar

- Ambiente da Demo 4 em execução.
- Atividades 1 e 2 concluídas (dados de `products` transformados no `datalake`).
- Consultas do README da Demo 4 como referência sintática.

---

## Passo a passo

### Passo 1 — Acessar o DuckDB

O container `dl-duckdb` usa a biblioteca DuckDB instalada via Python — o
acesso interativo é feito pelo shell Python, não pelo comando `duckdb`:

```bash
docker exec -it dl-duckdb python
```

Após entrar no shell Python, configure a conexão com o MinIO (execute
este bloco de uma vez antes de qualquer consulta):

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
```

### Passo 2 — Explorar os dados de Produtos

Com a conexão configurada, execute uma consulta para ler diretamente o
arquivo Parquet gerado na camada `processing` para a tabela `products`:

```python
con.sql("SELECT * FROM read_parquet('s3://datalake/processing/products/*.parquet') LIMIT 5").show()
```

### Passo 3 — Criar Consultas Analíticas

Elabore pelo menos duas consultas originais respondendo a perguntas
simples sobre os produtos (ex: encontrar produtos com determinada palavra
no nome ou ordenar por peso). Utilize filtros (`WHERE`) ou ordenações
(`ORDER BY`) básicos:

```python
con.sql("SELECT...'").show()
```

---

## O que entregar

- O texto das consultas SQL que você criou.
- Screenshot do terminal mostrando o resultado da execução das consultas.

---

## Dicas e erros comuns

- **Não use `docker exec -it dl-duckdb duckdb`** — esse comando falha
  porque o binário `duckdb` não está disponível. Use sempre
  `docker exec -it dl-duckdb python`.
- Lembre-se de configurar o bloco de conexão com o MinIO (Passo 1) a
  cada nova sessão, antes de executar qualquer consulta.
- Se o DuckDB acusar erro de arquivo não encontrado, verifique no MinIO
  se os caminhos e arquivos realmente existem.
