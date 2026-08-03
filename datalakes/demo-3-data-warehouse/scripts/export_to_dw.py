"""
Roteiro 4 — Exportando o processing para o Data Warehouse

Lê a camada processing de "customers" (bucket "datalake" no MinIO — gerada
pela demo 2, a partir do CDC da demo 1) e a exporta para um Data Warehouse
relacional (Postgres), como uma dimensão de clientes pronta para consultas
analíticas e para ser usada por outras equipes (BI, relatórios).

    Data Lake (processing/customers)    Data Warehouse (dim_clientes)
    ------------------------------------------------------------------
    arquivos parquet no MinIO       ->  tabela relacional no Postgres
    lido via engine de processamento -> consultado via SQL comum
    (Spark, DuckDB)                     (qualquer ferramenta de BI)
    "atualizado_em_ms" (epoch)      ->  "atualizado_em" (timestamp legível)

Rodar:
    docker exec -it dw-etl python export_to_dw.py
"""
import pandas as pd
from sqlalchemy import create_engine

MINIO_STORAGE_OPTIONS = {
    "key": "admin",
    "secret": "admin123",
    "client_kwargs": {"endpoint_url": "http://minio:9000"},
}
DW_URL = "postgresql+psycopg2://dw:dw@postgres-dw:5432/dw"

print("=" * 70)
print("ANTES — camada PROCESSING do Data Lake (parquet no MinIO)")
print("=" * 70)

processing = pd.read_parquet(
    "s3://datalake/processing/customers", storage_options=MINIO_STORAGE_OPTIONS
)
print(processing.to_string(index=False))
print(f"\nTotal de clientes: {len(processing)}")

# ---------------------------------------------------------------------
# Prepara a dimensão de clientes para o Data Warehouse
# ---------------------------------------------------------------------

dim_clientes = processing.copy()
dim_clientes["atualizado_em"] = pd.to_datetime(dim_clientes["atualizado_em_ms"], unit="ms")
dim_clientes = (
    dim_clientes.drop(columns=["atualizado_em_ms"])
    .rename(columns={"id": "cliente_id"})[
        ["cliente_id", "first_name", "last_name", "email", "ultima_operacao", "atualizado_em"]
    ]
    .sort_values("cliente_id")
)

print("\n" + "=" * 70)
print("DEPOIS — Data Warehouse (tabela dim_clientes)")
print("=" * 70)
print(dim_clientes.to_string(index=False))

# ---------------------------------------------------------------------
# Carrega no Postgres (Data Warehouse)
# ---------------------------------------------------------------------

engine = create_engine(DW_URL)
dim_clientes.to_sql("dim_clientes", engine, if_exists="replace", index=False)

print("\n" + "-" * 70)
print("Tabela gravada no Data Warehouse (Postgres): dim_clientes")
print("-" * 70)
