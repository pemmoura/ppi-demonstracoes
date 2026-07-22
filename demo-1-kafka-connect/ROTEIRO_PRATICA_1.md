RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Engenharia de Dados e Data Lakes**
**Atividade Prática 1 — Configuração do Ambiente**

---

## Objetivo

Configurar a captura de mudanças (CDC) na tabela `inventory.products` usando Debezium e persistir os eventos no MinIO, exigindo a adaptação autônoma das configurações vistas na demonstração.

---

## Antes de começar

- Ambiente da Demo 1 em execução.
- Acesso ao terminal e à interface web do MinIO.
- Arquivos de configuração da demonstração anterior.

---

## Passo a passo

### Passo 1 — Explorar a tabela products

Acesse o banco de dados e inspecione a estrutura e os dados da tabela `inventory.products` para entender o que será capturado.

```bash
docker exec -it dl-postgres psql -U postgres -d inventory
# Dica: Use os comandos \d e SELECT para explorar a tabela
```

### Passo 2 — Atualizar a configuração do Debezium

Edite o arquivo `debezium-postgres.json` adicionando `inventory.products` à propriedade de tabelas incluídas (`table.include.list`). Em seguida, utilize a API REST do Kafka Connect (via requisição PUT) para atualizar a configuração do conector, de forma similar ao que foi feito na demonstração anterior.

> ⚠️ **Após salvar o arquivo, aplique a configuração via API.** Editar o
> arquivo localmente não altera o conector em execução — é obrigatório
> enviar a requisição PUT ao Kafka Connect para que a mudança seja
> reconhecida. **Não é necessário reiniciar o Docker**, mas sem o PUT o
> Debezium continuará ignorando a tabela `products`.

### Passo 3 — Criar e registrar o S3 Sink Connector para Products

Crie um novo arquivo de configuração JSON com base no conector do S3 da demonstração. Adapte o nome do conector (para não haver conflito) e o tópico Kafka para apontar corretamente para a tabela de produtos. Envie o novo conector via método POST para a API do Kafka Connect.

> ⚠️ **Após criar o arquivo, registre o conector via API.** O Kafka Connect
> não detecta arquivos novos automaticamente — o arquivo JSON serve apenas
> como parâmetro da requisição POST. Sem enviar o POST, o conector não
> existirá e nenhum dado será gravado no MinIO.

### Passo 4 — Gerar alterações e validar no MinIO

Faça uma alteração de dados (UPDATE ou INSERT) na tabela `products` usando o `psql`. Em seguida, verifique se um novo diretório com o respectivo arquivo CDC foi gerado no bucket `raw` do MinIO. Execute o comando abaixo e verifique pela interface do MinIO.

```bash
# Liste os arquivos gerados no bucket raw:
docker exec -it dl-minio mc ls --recursive local/raw/cdc/
```

---

## Desafio extra

Adapte o script `generate_data.py` para também gerar uma sequência de eventos automatizada e contínua na tabela `products`.

---

## O que entregar

- O novo arquivo `s3-sink-raw-products.json` desenvolvido.
- Screenshot do MinIO mostrando o novo evento gravado na pasta de produtos.

---

## Dicas e erros comuns

- Se o conector S3 não gravar nada, confirme se o nome do tópico no arquivo JSON segue a exata convenção de nomenclatura do Debezium vista na teoria.
- Para debugar erros de conexão nos conectores, verifique os logs: `docker logs dl-connect`
