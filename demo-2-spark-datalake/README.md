# Demonstração 2 — Spark processa o bucket "raw" → camadas staging/processing no MinIO

Mostra a segunda etapa do pipeline: Spark lê os eventos CDC de `customers`
que chegaram no bucket `raw` do MinIO (gerados pela demo 1, via Kafka
Connect + Debezium + S3 Sink) e organiza esse dado no bucket `datalake`,
nas camadas staging e processing.

Corresponde aos slides "Demonstração — Data Lake com Spark e MinIO"
(Dia 1, Bloco 3).

## Pré-requisitos

- Docker e Docker Compose instalados
- A infraestrutura compartilhada precisa estar no ar (rede `ppi-net` +
  MinIO com os buckets `raw`/`datalake`):

  ```bash
  cd ../common
  docker compose up -d
  ```

- A demo 1 precisa ter rodado antes, incluindo o `generate_data.py`
  (é ele quem grava os eventos CDC em `raw/cdc/...`). Sem isso, esta
  demo não tem o que processar.

## Passo 1 — Subir o Spark

```bash
cd demo-2-spark-datalake
docker compose up -d
```

## Passo 2 — Ingerir a camada staging

```bash
docker exec -it dl-spark spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /scripts/ingest_staging.py
```

Confira o resultado no console do MinIO (**http://localhost:9001**,
usuário `admin`, senha `admin123`): bucket `datalake` → pasta
`staging/customers`. No terminal, repare que a staging é uma cópia fiel da
origem: cada linha é um evento CDC bruto (`before`/`after`/`op`/`ts_ms`),
exatamente como saiu da demo 1.

## Passo 3 — Gerar a camada processing

```bash
docker exec -it dl-spark spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /scripts/transform_processing.py
```

O script imprime um comparativo **ANTES (staging)** × **DEPOIS
(processing)** no terminal, mais um resumo do que mudou. Confira também o
resultado no console do MinIO: bucket `datalake` → pasta
`processing/customers`.

### O que muda do staging para o processing

| Staging (evento CDC cru) | Processing (dado tratado) |
| --- | --- |
| 1 evento por mudança (INSERT/UPDATE/DELETE) | 1 linha por cliente (só a versão mais recente) |
| Dado dentro do campo `after` | Colunas soltas (`id`, `first_name`, `last_name`, `email`) |
| Eventos de DELETE presentes | Removidos (cliente excluído não aparece no processing) |
| Nomes em CAIXA ALTA / minúsculas, com espaços duplicados/sobrando (`  MARIA   SILVA  `) | Padronizados em Proper Case, sem espaços (`Maria Silva`) |
| E-mail com espaços e caixa inconsistente (` GBailey@FOOBAR.COM `) | Sem espaços, em minúsculas (`gbailey@foobar.com`) |

## Passo 4 — Encerrar

```bash
docker compose down -v
```

> Isso encerra só o Spark desta demo. A infraestrutura compartilhada
> (`../common`, com o MinIO e os dados já processados) continua no ar
> para as próximas demos (3 e 4 leem exatamente o que ficou em `datalake`).

## Ideias para explorar em aula

- Rodar a staging e mostrar que ela é idêntica ao evento CDC visto no
  console consumer da demo 1 — nenhuma transformação ainda
- Rodar o processing e ler junto o comparativo ANTES × DEPOIS impresso no
  terminal, conferindo cada linha da tabela acima
- Mostrar os arquivos `.parquet` gerados dentro do console do MinIO
- Relacionar cada evento "sujo" gerado pelo `generate_data.py` (demo 1)
  com a linha correspondente, já limpa, no processing
- Perguntar: "o que aconteceria se eu rodasse `ingest_staging.py` de novo,
  sem gerar novos eventos?" para introduzir a ideia de reprocessamento a
  partir da staging
