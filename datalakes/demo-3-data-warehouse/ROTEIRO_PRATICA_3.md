RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Engenharia de Dados e Data Lakes**
**Atividade Prática 3 — Exportar para o Data Warehouse**

---

## Objetivo

Adaptar um script Python existente para exportar os dados da camada Processing para o Data Warehouse, e executar uma consulta SQL simples para validar a carga.

---

## Antes de começar

- Ambiente de contêineres Docker da Demo 3 em execução.
- Atividade 2 concluída (dados de `products` no `datalake/processing`).
- Script `export_to_dw.py` original disponível como referência na pasta
  `demo-3-data-warehouse/scripts/`.

---

## Passo a passo

### Passo 1 — Adaptar o Script de Exportação para o DW

Dentro da pasta `demo-3-data-warehouse/scripts/`, copie o arquivo
`export_to_dw.py` e renomeie para `export_products_to_dw.py`. Sua tarefa
é encontrar no código os parâmetros de origem e destino e alterá-los:
modifique a leitura para buscar os dados de `processing/products` e ajuste
a gravação para salvar na nova tabela chamada `dim_produtos`.

> ⚠️ **Após salvar o arquivo, reinicie o container do ETL** para que o novo
> script fique disponível dentro do container:
> ```bash
> docker compose restart etl
> ```

### Passo 2 — Execução do ETL

O script usa Python puro (pandas + s3fs), **não Spark** — execute-o no
container `dw-etl`, não no `dl-spark`:

```bash
docker exec -it dw-etl python export_products_to_dw.py
```

### Passo 3 — Consulta Analítica

Acesse o PostgreSQL do Data Warehouse. Faça uma query SQL simples (por
exemplo, um `SELECT` com `LIMIT` ou um `COUNT`) para garantir que os dados
dos produtos chegaram corretamente à nova tabela `dim_produtos`.

```bash
docker exec -it dw-postgres psql -U dw -d dw
# Use \dt para ver as tabelas criadas e elabore o seu SELECT.
```

---

## O que entregar

- O script `export_products_to_dw.py` adaptado por você.
- Uma screenshot da sua consulta SQL rodando no banco de dados `dw-postgres` com os resultados na tela.

---

## Dicas e erros comuns

- **Não use `spark-submit` nem o container `dl-spark`** para este script —
  ele não usa Spark, roda com Python comum no container `dw-etl`.
- O arquivo deve ficar em `demo-3-data-warehouse/scripts/` para ser
  visível dentro do container `dw-etl` (é a pasta montada como `/scripts`).
- Preste muita atenção no nome da tabela de destino no script para não
  sobrescrever acidentalmente outras tabelas já existentes no Data Warehouse.
