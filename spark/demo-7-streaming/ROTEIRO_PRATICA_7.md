RESIDÊNCIA TECNOLÓGICA EM INTELIGÊNCIA ARTIFICIAL
Programa Prioritário de Pesquisa, Desenvolvimento e Inovação — PPI Softex

# ROTEIRO DE ATIVIDADE PRÁTICA
**Processamento Big Data com Spark**
**Atividade Prática 7 — Streaming do log grande, com agregação por janela de tempo**

---

## Objetivo

Ampliar o streaming da demonstração para o log grande (`eventos_pedido.log`
+ `pedidos_grande.csv`) e adicionar uma agregação por janela de tempo (1
minuto), mantendo o enriquecimento via stream-static join.

---

## Antes de começar

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado.
- Ter reproduzido a demo 7 pelo menos uma vez, para entender o padrão
  `readStream.text` + `regexp_extract` + stream-static join.

---

## Passo a passo

### Passo 1 — Copiar e adaptar o script da demonstração

Copie `demo_streaming.py` para um novo arquivo (ex.:
`streaming_janela.py`). Troque as fontes estáticas para usar
`pedidos_grande.csv` (em vez de `pedidos_pequeno.csv`).

### Passo 2 — Simular a chegada do log grande

`simular_chegada_arquivos.py` aceita `--origem` para trocar o log de
entrada, sem precisar copiar o script:

```bash
docker exec -it spark-course python3 /curso/demo-7-streaming/scripts/simular_chegada_arquivos.py --origem /curso/common/dados/eventos_pedido.log --linhas-por-arquivo 20 --intervalo 4
```

### Passo 3 — Adicionar uma coluna de horário de chegada

Como o log tem um timestamp de evento, mas o mais simples para a janela é
usar o horário de processamento:

```python
stream_com_chegada = stream_enriquecido.withColumn("chegada", F.current_timestamp())
```

### Passo 4 — Agrupar por categoria E por janela de tempo

```python
contagem_por_janela = (
    stream_com_chegada
    .groupBy(F.window("chegada", "1 minute"), "categoria")
    .count()
)
```

---

## O que entregar

- Script `streaming_janela.py` rodando, com a contagem por categoria **e**
  por janela de 1 minuto sendo atualizada conforme o log grande chega
  (print/gravação do terminal mostrando pelo menos duas janelas
  diferentes).

---

## Envio

Junte as evidências de **todas** as atividades práticas do curso em um
único arquivo `.zip` nomeado como `Spark_Nome_do_Aluno.zip` e envie por
e-mail ao final da disciplina.

- **E-mail:** p.moura@sidi.org.br
- **Assunto:** `[Spark] Nome Da Pessoa`

---

## Dicas e erros comuns

- Agregações com janela de tempo exigem `checkpointLocation` — use uma
  pasta de checkpoint diferente da demo 7, para não misturar os dois.
- Se `categoria` aparecer sempre nula, confira se trocou a fonte estática
  para `pedidos_grande.csv` em todos os lugares (inclusive no join).
