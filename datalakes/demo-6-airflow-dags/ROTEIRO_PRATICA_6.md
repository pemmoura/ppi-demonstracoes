RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Engenharia de Dados e Data Lakes**
**Atividade Prática 6 — Estender o DAG real**

---

## Objetivo

Adaptar um DAG existente do Airflow para incluir o processamento da tabela de produtos de forma paralela ao fluxo de clientes, consolidando as automações feitas nas atividades anteriores por meio da cópia e adaptação de tasks.

---

## Antes de começar

- Ambiente da Demo 6 em execução.
- Scripts PySpark das atividades 2 e 3 salvos e funcionais.
- Acesso à interface web do Airflow.

---

## Passo a passo

### Passo 1 — Editar o DAG Original

Abra o arquivo `pipeline_datalake_real.py` localizado na pasta `dags/`. Este DAG atualmente orquestra apenas o processamento da tabela `customers`.

### Passo 2 — Adicionar Tarefas para Products

Duplique (copie e cole) as tarefas (`BashOperators`) de ingestão e transformação que já existem. Renomeie os `task_ids` e os scripts chamados dentro delas para apontar para as versões de `products` criadas por você na Atividade 2 (ex: `ingest_staging_products.py`).

> ⚠️ **Após salvar as alterações, reinicie o DAG Processor** para que o
> Airflow carregue a versão atualizada do arquivo:
> ```bash
> docker compose restart airflow-dag-processor
> ```

### Passo 3 — Ajustar as Dependências

Modifique a linha final do arquivo que define a ordem de execução. Faça
com que as novas tarefas de `products` rodem em paralelo com as de
`customers`, e que ambas precisem terminar antes de `validar_qualidade`.

> ⚠️ **Atenção — Airflow 3 não suporta `list >> list`**. A sintaxe
> `[task_a, task_b] >> [task_c, task_d]` causa `TypeError`. Use
> encadeamento explícito para cada pipeline:
> ```python
> task_ingerir >> task_transformar >> task_validar >> task_exportar
> task_ingerir_products >> task_transformar_products >> task_validar
> ```
> Dessa forma os dois pipelines correm em paralelo e se encontram
> na task `validar_qualidade`.

> ⚠️ **Após cada modificação no arquivo, reinicie o DAG Processor**
> para que o Airflow recarregue o DAG com as dependências atualizadas:
> ```bash
> docker compose restart airflow-dag-processor
> ```

### Passo 4 — Executar o Pipeline Completo

Na interface do Airflow, ative e dispare o DAG atualizado. Acompanhe a aba Graph para ver as tarefas de clientes e produtos rodando de forma orquestrada.

---

## O que entregar

- O arquivo `pipeline_datalake_real.py` modificado.
- Screenshot da aba Graph do Airflow mostrando as tarefas de clientes e produtos sendo executadas juntas.

---

## Dicas e erros comuns

- Tenha muito cuidado ao copiar as tarefas para não manter os mesmos `task_ids` (eles devem ser únicos obrigatoriamente dentro do mesmo DAG).
- Se uma tarefa falhar, clique nela na interface e vá em 'Log' para entender o que deu errado.
