RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Engenharia de Dados e Data Lakes**
**Atividade Prática 2 — Processar dados no Data Lake (Spark)**

---

## Objetivo

Adaptar scripts PySpark existentes para ingerir dados brutos da tabela `products` na camada Staging e transformá-los na camada Processing, compreendendo o fluxo de dados sem a necessidade de programar do zero em Spark.

---

## Antes de começar

- Ambiente de contêineres Docker da Demo 2 em execução.
- Atividade 1 concluída (dados de `products` no `raw` do MinIO).
- Scripts `ingest_staging.py` e `transform_processing.py` originais disponíveis.

---

## Passo a passo

### Passo 1 — Script de Ingestão (Staging)

Faça uma cópia do arquivo `ingest_staging.py` e renomeie para `ingest_staging_products.py`. Como o foco não é programar em Spark, sua tarefa é apenas encontrar no código os caminhos de leitura e escrita e alterá-los para ler da pasta `raw` de `products` e gravar na pasta `staging` de `products`.

> ⚠️ **Após salvar o arquivo, reinicie o container do Spark** para garantir
> que ele enxerga o novo script no volume montado:
> ```bash
> docker compose restart spark
> ```

### Passo 2 — Script de Transformação (Processing)

Copie o `transform_processing.py` e renomeie para `transform_processing_products.py`. Novamente, altere os caminhos de origem e destino (de `customers` para `products`).

> ⚠️ **Após salvar o arquivo, reinicie o container do Spark** novamente
> antes de executar o script:
> ```bash
> docker compose restart spark
> ``` Não é necessário criar regras complexas de qualidade de dados: apenas ajuste as colunas que estão sendo selecionadas para corresponder às colunas reais da tabela `products` (removendo referências exclusivas aos clientes).

### Passo 3 — Executar e Validar

Rode os dois scripts utilizando o `spark-submit` dentro do contêiner do Spark e verifique se as novas pastas apareceram no MinIO com dados populados.

```bash
docker exec -it dl-spark spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /scripts/ingest_staging_products.py
```
```bash
docker exec -it dl-spark spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /scripts/transform_processing_products.py
```

---

## O que entregar

- Os dois scripts PySpark adaptados.
- Screenshot do console do MinIO mostrando os arquivos criados em `staging` e `processing`.

---

## Dicas e erros comuns

- Você não precisa saber Spark a fundo: busque no código original os locais onde a tabela antiga (`customers`) é mencionada e substitua com atenção.
- Lembre-se de verificar se não ficaram nomes de caminhos antigos esquecidos no código copiado, o que causaria erros de leitura.
