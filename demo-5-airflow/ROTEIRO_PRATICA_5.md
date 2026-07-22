RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Engenharia de Dados e Data Lakes**
**Atividade Prática 5 — Criar um pipeline simples no Airflow**

---

## Objetivo

Compreender a estrutura básica de um DAG (Directed Acyclic Graph) no Apache Airflow, criando tarefas simples e definindo dependências de execução, sem focar em códigos de processamento pesados.

---

## Antes de começar

- Ambiente da Demo 5 em execução.
- Acesso à interface web do Airflow (http://localhost:8080).
- Arquivo `pipeline_datalake.py` original como referência de estrutura.

---

## Passo a passo

### Passo 1 — Estrutura Base do DAG

Crie um novo arquivo chamado `meu_pipeline.py` na pasta `dags/`. Copie as importações e a declaração básica do DAG do pipeline da demonstração, alterando o nome do DAG (`dag_id`) para algo único.

> ⚠️ **Após salvar o arquivo, reinicie o DAG Processor** para que o Airflow
> carregue o novo DAG imediatamente sem precisar aguardar o ciclo de
> detecção automática:
> ```bash
> docker compose restart airflow-dag-processor
> ```

### Passo 2 — Definir Tarefas (Tasks)

Crie 3 tarefas simples usando o `BashOperator` (por exemplo: um `echo "Iniciando"`, `echo "Processando"`, `echo "Finalizando"`). O objetivo aqui é apenas focar na orquestração.

### Passo 3 — Configurar Dependências

Defina a ordem de execução das tarefas no final do arquivo. Tente fazer com que a primeira tarefa acione as outras duas em paralelo (ex: `task1 >> [task2, task3]`).

> ⚠️ **Após cada modificação no arquivo, reinicie o DAG Processor** para
> que o Airflow recarregue o DAG com as alterações mais recentes:
> ```bash
> docker compose restart airflow-dag-processor
> ```

### Passo 4 — Validar no Airflow

Abra a interface do Airflow, localize o seu DAG, ative-o (botão toggle) e dispare uma execução manual. Verifique se o desenho das dependências aparece corretamente na aba Graph.

---

## O que entregar

- O arquivo `meu_pipeline.py` desenvolvido.
- Screenshot da interface do Airflow (Graph ou Grid View) mostrando a execução do seu DAG.

---

## Dicas e erros comuns

- O Airflow demora alguns segundos para detectar um DAG novo. Aguarde um pouco ou atualize a página.
- Erros de sintaxe no Python farão o DAG não aparecer na interface principal. Verifique a aba de 'Import Errors' se isso acontecer.

---

> 🔴 **Antes de iniciar a Demonstração 6 ou a Atividade 6**, encerre o
> Airflow desta atividade. As duas demos usam a porta **8080** — rodá-las
> ao mesmo tempo causa conflito:
> ```bash
> docker compose down -v
> ```
