# Demonstração 5 — Join de 3 tabelas em SQL, CASE WHEN, funções de data/texto

Registra `pedidos`, `clientes` e `produtos` (já sem a duplicidade de
`produto_id`) como views temporárias e mostra os recursos de SQL mais
usados no dia a dia: join de 3 tabelas, `CASE WHEN` para criar faixas,
funções de data (`YEAR`, `MONTH`, `DATEDIFF`) e de texto (`UPPER`, `TRIM`,
`CONCAT`). Fecha comparando uma consulta com o equivalente via DataFrame
API.

Corresponde aos slides "Demonstração — Bloco 2" (Dia 2, Bloco 2: Spark SQL
básico), incorporando também "JOIN em SQL" e "Funções úteis em SQL" (do
mesmo bloco).

## Pré-requisitos

- Infraestrutura compartilhada no ar e `gerar_dados.py` já executado — os
  arquivos `common/dados/pedidos_pequeno.csv`, `common/dados/clientes.csv`
  e `common/dados/produtos.json` precisam existir.

## Passo único — Rodar a demonstração

```bash
docker exec -it spark-course spark-submit /curso/demo-5-spark-sql/scripts/demo_spark_sql.py
```

## Ideias para explorar em aula

- Reforçar que `produtos` foi registrada **já sem duplicidade**
  (`dropDuplicates` antes do `createOrReplaceTempView`) — o problema visto
  na demo 4 precisa ser resolvido antes de virar view, o SQL não faz isso
  sozinho
- Mostrar que pedidos sem `cliente_id` ou com `produto_id` inexistente
  simplesmente somem do resultado do `JOIN` (inner join, padrão) — sem
  erro, sem aviso
- Ler o `CASE WHEN` em voz alta como um "se/senão" comum — costuma
  desmistificar rápido para quem nunca viu SQL
- Perguntar qual das duas formas (SQL ou DataFrame) parece mais natural
  para quem já usa banco de dados relacional
