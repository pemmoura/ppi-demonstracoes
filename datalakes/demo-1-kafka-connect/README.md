# Demonstração 1 — Kafka Connect e CDC com Debezium → bucket "raw" do MinIO

Mostra como capturar mudanças em um banco Postgres em tempo real usando
Kafka Connect + o conector Debezium, e como levar esse dado bruto até o
Data Lake (bucket `raw` do MinIO) usando **outro** conector Kafka Connect
(S3 Sink), sem escrever nenhum código de integração.

Corresponde aos slides "Demonstração — Kafka Connect e CDC com Debezium"
(Dia 1, Bloco 1). Esta é a **primeira etapa do pipeline completo** do
curso — o que ela grava no MinIO é o que a demo 2 (Spark) vai processar.

## Pré-requisitos

- Docker e Docker Compose instalados
- Portas livres: 2181, 5432, 8083, 9092
- A infraestrutura compartilhada precisa estar no ar (rede `ppi-net` +
  MinIO com os buckets `raw`/`datalake`):

  ```bash
  cd ../common
  docker compose up -d
  ```

## Passo 1 — Subir o ambiente

```bash
cd demo-1-kafka-connect
docker compose up -d --build
docker compose ps
```

O `--build` é necessário na primeira vez (e sempre que o `Dockerfile.connect`
mudar): ele monta uma imagem de Kafka Connect com dois plugins instalados
via `confluent-hub` — o conector Debezium (Postgres) e o conector S3 Sink
da Confluent. Aguarde todos os serviços aparecerem como `running`/`healthy`;
a primeira vez pode levar alguns minutos (build da imagem + download).

## Passo 2 — Registrar o conector Debezium (origem: Postgres → Kafka)

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium-postgres.json
```

Verifique se o conector está rodando:

```bash
curl -s localhost:8083/connectors/inventory-connector/status | python3 -m json.tool
```

## Passo 3 — Registrar o conector S3 Sink (destino: Kafka → bucket "raw" do MinIO)

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @s3-sink-raw.json
```

Verifique:

```bash
curl -s localhost:8083/connectors/raw-sink-connector/status | python3 -m json.tool
```

> A config já inclui `"behavior.on.null.values": "ignore"`. Sem isso, a
> primeira exclusão (DELETE) gerada pelo `generate_data.py` mata a task do
> conector: todo DELETE no Postgres faz o Debezium emitir, logo em
> seguida, um registro "tombstone" (valor `null`) para log compaction do
> Kafka, e o S3 Sink por padrão trata isso como erro fatal e para de
> consumir — sintoma típico: alguns arquivos chegam no bucket `raw` e
> depois param de vir, mesmo com o Postgres continuando a receber eventos.
> Se isso acontecer, confira `state` da task em `.../status` — se estiver
> `"FAILED"`, apague e recrie o conector (`curl -X DELETE
> localhost:8083/connectors/raw-sink-connector` seguido do POST acima).

## Passo 4 — Ver os eventos chegando

Em um terminal, comece a consumir o tópico gerado pelo Debezium para a
tabela `customers`:

```bash
docker exec -it dl-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.inventory.customers \
  --from-beginning
```

Em outro terminal, altere um dado no Postgres. A imagem `debezium/example-postgres`
cria as tabelas de exemplo dentro do **schema** `inventory` (não em `public`),
por isso é preciso qualificar `inventory.customers`:

```bash
docker exec -it dl-postgres psql -U postgres -d inventory -c \
  "UPDATE inventory.customers SET first_name = 'Maria' WHERE id = 1001;"
```

Você verá, no terminal do consumidor, um evento JSON aparecer quase
imediatamente, mostrando o dado **antes** e **depois** da mudança.

## Passo 5 — Gerar um lote variado de eventos (necessário antes da demo 2)

A demo 2 (Spark) processa o que estiver em `raw/cdc/...` — sem eventos
gerados aqui, não há nada para ela processar. Rode o script abaixo, que
gera, por padrão, **1000 eventos** de INSERT/UPDATE/DELETE ao longo de
**~5 minutos**, com problemas de qualidade de dado de propósito (espaços
sobrando, nomes em CAIXA ALTA/minúsculas, e-mail com espaços), para a
demo 2 mostrar uma diferença visível entre staging e processing, agora
com volume real de dado:

```bash
python generate_data.py
```

Acompanhe os eventos chegando no console consumer aberto no Passo 4 (o
terminal vai rolar rápido — é esperado). Para ajustar o volume/duração:

```bash
python generate_data.py --total 200 --duration 60   # mais rápido, útil para testes
python generate_data.py --total 1000 --duration 0   # sem pausas, o mais rápido possível
```

Use `--container`/`--db` se você tiver renomeado o container ou o banco.

## Passo 6 — Conferir o dado bruto no MinIO

Acesse o console do MinIO em **http://localhost:9001**
(usuário: `admin`, senha: `admin123`) e abra o bucket `raw` → pasta
`cdc/dbserver1.inventory.customers/`. Cada evento CDC vira um arquivo
JSON — exatamente o dado bruto, sem nenhuma transformação, que a demo 2
(Spark) vai consumir para gerar a camada staging.

## Passo 7 — Encerrar

```bash
docker compose down -v
```

> Isso encerra só os serviços desta demo (Kafka, Postgres, Connect). A
> infraestrutura compartilhada (`../common`, com o MinIO) continua no ar
> para as próximas demos.

## Ideias para explorar em aula

- Inserir uma linha nova e comparar o evento de INSERT com o de UPDATE
- Mostrar o schema do evento (campos `before`, `after`, `op`, `ts_ms`)
- Abrir o arquivo JSON gerado no bucket `raw` e relacionar cada campo com
  o evento visto no console consumer
- Explicar que dois conectores (origem e destino) rodando no mesmo Kafka
  Connect é o padrão de uso real da ferramenta: ela não processa dados,
  só os move entre sistemas
- Depois de rodar `generate_data.py`, abrir um dos eventos "sujos" no MinIO
  (procure algum com nome em CAIXA ALTA ou espaços sobrando — com 1000
  eventos, é fácil achar) e perguntar: "isso vai continuar assim no
  processing?" — gancho direto para a limpeza de qualidade que a demo 2
  faz em cima de `customers`
