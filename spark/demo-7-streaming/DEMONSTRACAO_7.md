# Demonstração 7 — Streaming de um log de aplicação + stream-static join

Monitora uma pasta (`pasta_entrada/`) que recebe, aos poucos, arquivos de
um log de aplicação real (`eventos_pequeno.log`, dividido em pedaços por
`simular_chegada_arquivos.py`) — o caso de uso clássico de streaming.
Cada linha do log só tem `pedido_id`/`evento`/`nivel`; para saber a
categoria do produto e o cliente por trás de cada erro, o stream é
enriquecido em tempo real com um **stream-static join** contra
DataFrames estáticos (`pedidos`, `produtos`, `clientes`).

Corresponde aos slides "Demonstração — Bloco 1" (Dia 3, Bloco 1:
Introdução ao streaming), usando a fonte "monitorar uma pasta" (slide
"Fontes comuns de streaming") com um log real em vez de arquivos
genéricos.

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/eventos_pequeno.log`,
  `common/dados/pedidos_pequeno.csv`, `common/dados/produtos.json` e
  `common/dados/clientes.csv` precisam existir.

## Passo 1 — Iniciar o streaming (terminal 1)

```bash
docker exec -it spark-course spark-submit /curso/demo-7-streaming/scripts/demo_streaming.py
```

O script carrega as fontes estáticas, fica monitorando `pasta_entrada/` e
processa micro-lotes (vazios, no começo) mesmo antes do primeiro arquivo
chegar — é normal ver várias tabelas vazias no início.

## Passo 2 — Simular a chegada do log (terminal 2)

```bash
docker exec -it spark-course python3 /curso/demo-7-streaming/scripts/simular_chegada_arquivos.py
```

Por padrão, divide o log em pedaços de 4 linhas, um arquivo novo a cada 5
segundos (~8 arquivos, ~40s no total). Acompanhe, no terminal 1, a
contagem de eventos e de erros por categoria sendo atualizada a cada novo
arquivo — a tabela deixa de ficar vazia assim que o **primeiro** arquivo
chega (a agregação conta todos os eventos, não só os de erro: com só 30
linhas de log, `eventos_pequeno.log` tem só 1 evento de erro, então
esperar só por ele deixaria a demonstração frágil).

## Passo 3 — Encerrar (e como reiniciar do zero)

`Ctrl+C` no terminal 1 encerra o streaming. Para reiniciar, é
**obrigatório** limpar `pasta_entrada/` e `checkpoint/` antes:

```bash
docker exec -it spark-course bash -c "rm -rf /curso/demo-7-streaming/pasta_entrada/* /curso/demo-7-streaming/checkpoint/*"
```

Isso porque o `checkpointLocation` guarda quais arquivos já foram
processados **pelo nome do arquivo** (`eventos_001.log`,
`eventos_002.log`, ...). Se você rodar `simular_chegada_arquivos.py` de
novo sem limpar, ele recria arquivos com os mesmos nomes, e o Spark os
ignora silenciosamente por achar que já processou — sintoma: o streaming
roda, mostra batches, mas a tabela nunca sai do vazio.

## Ideias para explorar em aula

- Comparar com a demo 6 (`spark.read.text` em modo batch): aqui é
  `spark.readStream.text`, a mesma ideia, só que contínua
- Explicar o stream-static join: o lado que streama (o log) fica à
  esquerda do `join`; o lado estático (`pedido_para_categoria_cliente`)
  não muda durante a execução — é assim que o Spark sabe que só precisa
  reprocessar o lado que streama a cada novo lote
- Abrir a Spark UI (http://localhost:4040) durante a execução e mostrar a
  aba "Structured Streaming", com o histórico de micro-lotes processados
- Perguntar: "por que a categoria aparece como nula para alguns erros?" —
  são os mesmos pedidos com `produto_id` inexistente vistos na demo 4
