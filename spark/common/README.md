# Infraestrutura compartilhada — "Passo 0" do curso de Spark

Sobe o único container usado pelas 9 demos: Spark rodando localmente
(`apache/spark-py:v3.4.0`), com a pasta `spark/` inteira montada em
`/curso`. Suba isto **uma vez**, no início do curso, e deixe rodando.

## Rodando sem instalar nada localmente (Codespaces / Cloud Shell)

Se não quiser instalar Docker na própria máquina, o curso inteiro roda
igual em dois ambientes gratuitos com terminal + Docker no navegador. Os
passos de **Subir**, **Gerar os dados** e o resto deste README são os
mesmos nos dois casos — só muda como você abre o terminal e como acessa a
Spark UI (porta 4040), explicado nas seções específicas abaixo.

### GitHub Codespaces (https://github.com/codespaces)

1. No GitHub, abra o repositório `ppi-demonstracoes` → botão **Code** →
   aba **Codespaces** → **Create codespace on main** (ou acesse
   [github.com/codespaces](https://github.com/codespaces) e crie um
   codespace a partir deste repositório).
2. Aguarde o ambiente terminar de provisionar — abre um VS Code no
   navegador, já com Docker instalado.
3. Abra um terminal (menu **Terminal → New Terminal**, ou `` Ctrl+` ``) e
   siga normalmente a partir de `cd spark/common`.
4. **Spark UI**: quando um `spark-submit` estiver rodando, o Codespaces
   detecta a porta 4040 aberta e mostra uma notificação
   "Your application running on port 4040 is available" — clique nela, ou
   abra a aba **Ports** (painel inferior) e clique no ícone de globo ao
   lado da porta `4040`.
5. Ao terminar a aula, **pare o codespace** (menu do codespace → *Stop
   codespace*) para não consumir as horas gratuitas do plano. Ao retomar
   depois, será preciso rodar `docker compose up -d` de novo (os
   containers não continuam rodando com o codespace parado).

### Google Cloud Shell (https://shell.cloud.google.com/)

1. Abra o link acima com uma conta Google — não é preciso ter um projeto
   GCP configurado só para isso, pode pular a criação de projeto se for
   oferecida.
2. Aguarde a VM temporária provisionar; ela já vem com Docker instalado.
3. Clone o repositório (o Cloud Shell não abre automaticamente a partir de
   um repositório específico) e entre na pasta:

   ```bash
   git clone https://github.com/pemmoura/ppi-demonstracoes.git
   cd ppi-demonstracoes/spark/common
   ```

4. Siga normalmente a partir do **Subir** abaixo.
5. **Spark UI**: o Cloud Shell não expõe `localhost` diretamente — use o
   botão **Web Preview** (ícone de olho, no canto superior direito do
   terminal) → **Change port** → digite `4040` → **Change and Preview**.
   Repita isso a cada novo `spark-submit`, já que a UI só existe enquanto
   o job está rodando.
6. Atenção: a sessão do Cloud Shell se desconecta após ~20 minutos de
   inatividade no navegador (a VM em si dura até 24h, mas os containers
   Docker não sobrevivem entre sessões) — se isso acontecer, repita os
   passos de **Subir**/**Gerar os dados** ao reconectar.

## Pré-download da imagem (recomendado antes da aula)

```bash
docker pull apache/spark-py:v3.4.0
```

## Subir

```bash
cd common
docker compose up -d
```

> Se você já tinha o container `spark-course` no ar de antes (de uma
> versão anterior deste `docker-compose.yml`), recrie-o para pegar o
> volume novo do `log4j2.properties`:
> `docker compose up -d --force-recreate`.

## Logs do Spark (nível WARN por padrão)

`common/log4j2.properties` é montado em `/opt/spark/conf/log4j2.properties`
dentro do container e deixa o nível de log padrão em `WARN`, não `INFO` —
assim, qualquer `spark-submit` de qualquer demo já sai com bem menos ruído
(sem precisar de flag na linha de comando nem de
`spark.sparkContext.setLogLevel("WARN")` no script). Os `print()` de cada
demo e erros reais continuam aparecendo normalmente.

## Gerar os dados do curso

Depois do container no ar, gere as 6 fontes de dados usadas no curso
inteiro (veja `scripts/gerar_dados.py` para os detalhes):

```bash
docker exec -it spark-course python3 /curso/common/scripts/gerar_dados.py
```

Isso modela uma loja fictícia com **4 fontes em formatos diferentes**, de
propósito, para praticar leitura de CSV, JSON e log de texto, e joins
entre elas:

| Arquivo | Formato | Papel |
| --- | --- | --- |
| `clientes.csv` | CSV | dimensão cliente (`cliente_id, nome, cidade, regiao, segmento`) |
| `produtos.json` | JSON (um objeto por linha) | dimensão produto (`produto_id, produto, categoria, preco_base, fornecedor`) — pequena, boa candidata a **broadcast join** |
| `pedidos_pequeno.csv` (10 linhas) / `pedidos_grande.csv` (300 linhas) | CSV | fato (`id, cliente_id, produto_id, quantidade, valor_total, data`) — usados nas demonstrações e nas atividades, respectivamente |
| `eventos_pequeno.log` (30 linhas) / `eventos_pedido.log` (800 linhas) | texto, log de aplicação | eventos do ciclo de vida de cada pedido (`CRIADO`, `PAGAMENTO_APROVADO`, `PAGAMENTO_RECUSADO`, `ESTOQUE_INSUFICIENTE`, `ENVIADO`, `ENTREGUE`, `CANCELADO`) |

Os pedidos só têm `cliente_id`/`produto_id` — para saber o nome do
cliente ou do produto, é preciso fazer **join** com `clientes.csv`/
`produtos.json`. De propósito, os dados têm algumas inconsistências reais
para praticar tratamento de dado:

- alguns pedidos têm `cliente_id` vazio (nulo) — pratica `dropna()`/`fillna()`
- alguns pedidos e eventos de log referenciam um `produto_id`/`pedido_id`
  que não existe — pratica a diferença entre inner e left join
- o `produto_id` 1 (Mouse) aparece **duas vezes** em `produtos.json`, com
  fornecedores diferentes — mostra como um join mal feito duplica linhas
  (slide "Introdução a joins")

## Rodando um script de qualquer demo

Todo script de qualquer `demo-N-*/scripts/` é acessível dentro do
container em `/curso/demo-N-*/scripts/`, por exemplo:

```bash
docker exec -it spark-course spark-submit /curso/demo-1-intro-spark/scripts/demo_sparksession_lazy.py
```

## Acompanhando a Spark UI

Enquanto um `spark-submit` está rodando, a interface fica disponível em
**http://localhost:4040**. Ela fecha assim que o script termina — útil
principalmente na demo 8 (Spark UI: jobs, stages e tasks).

Rodando em Codespaces ou Cloud Shell em vez do próprio computador? Veja
"Rodando sem instalar nada localmente" no início deste README — o acesso à
porta 4040 é diferente em cada um (aba **Ports** no Codespaces, **Web
Preview** no Cloud Shell), já que `localhost` ali aponta para a máquina
remota, não para o seu navegador.

## Encerrar (só no final do curso)

```bash
cd common
docker compose down -v
```
