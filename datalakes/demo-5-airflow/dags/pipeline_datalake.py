"""
DAG de exemplo — pipeline_datalake

Representa, de forma simplificada, o pipeline construído ao longo da
disciplina: ingerir -> transformar -> salvar, com uma validação de
qualidade antes de finalizar.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def ingerir_dados():
    print("Ingerindo dados brutos na camada staging...")


def transformar_dados():
    print("Limpando e organizando os dados na camada processing...")


def validar_qualidade():
    # Exemplo simples: checagem de completude
    registro = {"cliente": "Ana Souza", "valor": 1200.50}
    assert registro.get("cliente"), "Campo 'cliente' não pode estar vazio"
    print("Validação de qualidade: OK")


def consolidar_resultado():
    print("Gerando agregação final a partir da camada processing...")


with DAG(
    dag_id="pipeline_datalake",
    description="Pipeline de exemplo do curso: ingerir -> transformar -> validar -> salvar",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",  # todos os dias às 6h
    catchup=False,
    tags=["ppi-softex", "data-lakes"],
) as dag:

    task_ingerir = PythonOperator(
        task_id="ingerir",
        python_callable=ingerir_dados,
    )

    task_transformar = PythonOperator(
        task_id="transformar",
        python_callable=transformar_dados,
    )

    task_validar = PythonOperator(
        task_id="validar_qualidade",
        python_callable=validar_qualidade,
    )

    task_salvar = PythonOperator(
        task_id="consolidar_resultado",
        python_callable=consolidar_resultado,
    )

    task_ingerir >> task_transformar >> task_validar >> task_salvar
