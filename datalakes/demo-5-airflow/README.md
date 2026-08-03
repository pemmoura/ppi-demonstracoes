# Demonstração 5 — Apache Airflow com Docker

Sobe um Airflow 3.3.0 local (API Server + Scheduler + DAG Processor + banco
de metadados) e carrega um DAG de exemplo (com tasks ilustrativas, só
`print()`) para explorar a interface e os conceitos do Airflow: DAGs,
tasks, agendamento, Grid View, logs. O DAG que orquestra de verdade as
demos 1-4 fica na demo 6, depois que os conceitos básicos já tiverem sido
explorados aqui.

Corresponde aos slides "Demonstração — Apache Airflow com Docker"
(Dia 2, Bloco 2).

> Esta configuração usa o `LocalExecutor` e é simplificada para fins
> didáticos. Para produção, a documentação oficial do Airflow recomenda
> o `docker-compose.yaml` completo (com Redis e CeleryExecutor).
>
> A partir do Airflow 3, o antigo "Webserver" virou "API Server" (a UI é
> servida por ele) e o processamento de DAGs saiu do Scheduler para um
> serviço próprio, o "DAG Processor" — obrigatório mesmo em setups simples
> como este. É por isso que agora sobem 4 serviços (banco + 3 componentes
> do Airflow) em vez de 3.

## Pré-requisitos

- Docker e Docker Compose instalados
- Porta livre: 8080
- Pelo menos 4 GB de RAM disponíveis para o Docker (com 4 serviços do
  Airflow no ar, prefira 6-8 GB se possível)

## Passo 1 — Inicializar o banco de metadados

```bash
cd demo-5-airflow
docker compose up airflow-init
```

Aguarde a mensagem de criação do usuário `airflow` e o comando encerrar
sozinho.

## Passo 2 — Subir o API Server, o Scheduler e o DAG Processor

```bash
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor
```

Acesse **http://localhost:8080** (usuário: `airflow`, senha: `airflow`).

## Passo 3 — Ativar e executar o DAG

1. Na lista de DAGs, localizar `pipeline_datalake`
2. Ativar o toggle ("on") ao lado do nome
3. Clicar no DAG → botão "Trigger DAG" (ícone de play) para disparar uma
   execução manual
4. Acompanhar o progresso pela "Grid View"
5. Clicar em qualquer task (ex: `ingerir`) → "Logs" para ver a saída

## Passo 4 — Encerrar

```bash
docker compose down -v
```

> ⚠️ **Antes de começar a demo 6:** encerre o Airflow desta demo com o
> comando acima (`docker compose down -v`) antes de subir a demo 6.
> Ambas usam a porta **8080** — se as duas estiverem no ar ao mesmo
> tempo, a demo 6 vai falhar ao subir o API Server com conflito de porta.

## Ideias para explorar em aula

- Mostrar a dependência `>>` no código e relacionar com a ordem de
  execução visível na Grid View
- Forçar um erro (ex: comentar o `assert` de `validar_qualidade` e trocar
  por uma condição que falhe) para mostrar uma task em vermelho e seus logs
- Comparar `schedule="0 6 * * *"` com a explicação de cron do slide
- Mostrar os 3 componentes do Airflow (`docker compose ps`) e relacionar
  cada um com sua função: API Server (UI/API), Scheduler (decide quando
  rodar) e DAG Processor (lê e interpreta os arquivos de DAG)
- Encerrar apontando que este DAG só simula o pipeline (`print()`); a demo 6
  troca isso por tasks que de fato rodam o Spark (demo 2) e o export para o
  Data Warehouse (demo 3)
