# Demonstração 6 — DAGs reais de processamento de dados

Depois de explorar os conceitos do Airflow na demo 5 (com um DAG que só
imprimia mensagens), esta demo sobe um segundo Airflow com um DAG que
**de fato** executa o pipeline em lote das demos anteriores:

```
ingerir_staging -> transformar_processing -> validar_qualidade -> exportar_data_warehouse
  (Spark,             (Spark,              (DuckDB, consulta       (script da
   demo 2)             demo 2)              o parquet no MinIO)     demo 3)
```

Nenhuma lógica é duplicada: o Airflow monta os mesmos scripts da demo 2
(`ingest_staging.py`, `transform_processing.py`) e da demo 3
(`export_to_dw.py`) como volumes e os executa via `spark-submit`/`python`.
A imagem do Airflow
ganhou PySpark e DuckDB (veja o `Dockerfile`) para conseguir rodar isso
sozinha, sem precisar entrar em outros containers.

> A ingestão via CDC (demo 1) continua contínua/streaming e não faz parte
> deste DAG — aqui o Airflow orquestra só a parte em lote (Spark + Data
> Warehouse), que roda periodicamente (`schedule="0 6 * * *"`).
>
> Usa Airflow 3.3.0: o antigo "webserver" virou "api-server" e existe um
> serviço à parte, o "dag-processor", obrigatório mesmo em setups simples.

> ⚠️ **Atenção antes de começar:** a demo 5 e esta demo usam a mesma
> porta **8080**. Certifique-se de que o Airflow da demo 5 já foi
> encerrado com `docker compose down -v` dentro de `demo-5-airflow`
> antes de subir qualquer serviço aqui. Tentar rodar as duas ao mesmo
> tempo vai causar conflito de porta ao subir o API Server.

## Pré-requisitos

- Docker e Docker Compose instalados
- Porta livre: 8080 (encerrar a demo 5 antes de subir esta)
- Pelo menos 4 GB de RAM disponíveis para o Docker (com mais serviços no
  ar, prefira 6-8 GB se possível)
- Precisam estar de pé:
  - `../common` (rede `ppi-net` + MinIO)
  - `../demo-2-spark-datalake` (container `dl-spark`)
  - `../demo-3-data-warehouse` (container `dw-postgres`)

## Passo 1 — Inicializar o banco de metadados

```bash
cd demo-6-airflow-dags
docker compose up --build airflow-init
```

O `--build` é necessário na primeira vez (e sempre que o `Dockerfile`
mudar), para instalar PySpark e DuckDB na imagem do Airflow.

## Passo 2 — Subir o API Server, o Scheduler e o DAG Processor

```bash
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor
```

Acesse **http://localhost:8080** (usuário: `airflow`, senha: `airflow`).

## Passo 3 — Ativar e executar o DAG

1. Localizar `pipeline_datalake_real` na lista de DAGs
2. Ativar o toggle ("on")
3. "Trigger DAG" para disparar uma execução manual
4. Acompanhar as 4 tasks na Grid View — cada uma reflete um passo já visto
   manualmente nas demos 2 e 3
5. Abrir os logs da task `ingerir_staging` ou `transformar_processing` e apontar
   que a saída é **idêntica** à que apareceu no terminal quando rodamos
   `spark-submit` manualmente na demo 2

## Passo 4 — Conferir o resultado

```bash
docker exec -it dw-postgres psql -U dw -d dw -c "SELECT * FROM dim_clientes LIMIT 5;"
```

## Passo 5 — Encerrar

```bash
docker compose down -v
```

## Ideias para explorar em aula

- Comparar este DAG com o da demo 5: mesma estrutura (`>>`), mas agora as
  tasks fazem trabalho de verdade
- Forçar uma falha (ex: derrubar o `dl-spark` antes de rodar o DAG) e
  mostrar a task `ingerir_staging` em vermelho, com o erro de conexão nos logs
- Discutir por que a task `validar_qualidade` existe: um "portão de
  qualidade" antes de exportar para o Data Warehouse, útil para não propagar
  dado ruim adiante
- Fechar o curso relacionando o diagrama completo do pipeline (README da
  raiz do projeto) com o que cada uma das 6 demos representou
