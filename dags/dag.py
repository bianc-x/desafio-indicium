from __future__ import annotations

import pendulum
import pandas as pd
import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extrair_csv_para_datalake(**kwargs):
    origem = "transacoes.csv" 
    
    caminho_datalake = "/opt/airflow/dags/data/raw"
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    caminho_destino = f"{caminho_datalake}/{data_hoje}/transacoes"
    os.makedirs(caminho_destino, exist_ok=True)
    
    caminho_arquivo_destino = f"{caminho_destino}/transacoes.csv"

    df = pd.read_csv(f"/opt/airflow/dags/data/source/{origem}")

    df.to_csv(caminho_arquivo_destino, index=False)

def extrair_sql_para_datalake(**kwargs):
    hook = PostgresHook(postgres_conn_id="banvic_datawarehouse_conn")

    queries = {
        "propostas_credito": "SELECT * FROM public.propostas_credito;",
        "contas": "SELECT * FROM public.contas;"
    }

    caminho_datalake = "/opt/airflow/dags/data/raw"
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    caminho_destino = f"{caminho_datalake}/{data_hoje}/tabelas_sql"
    os.makedirs(caminho_destino, exist_ok=True)
    
    for tabela, query in queries.items():
        df = hook.get_pandas_df(sql=query)
        
        caminho_arquivo_destino = f"{caminho_destino}/{tabela}.csv"
        df.to_csv(caminho_arquivo_destino, index=False)

def carregar_para_data_warehouse(**kwargs):
    hook_dw = PostgresHook(postgres_conn_id="banvic_datawarehouse_conn")

    data_hoje = datetime.now().strftime("%Y-%m-%d")
    caminho_raw = f"/opt/airflow/dags/data/raw/{data_hoje}"

    caminho_transacoes = f"{caminho_raw}/transacoes/transacoes.csv"
    df_transacoes = pd.read_csv(caminho_transacoes)
    hook_dw.insert_rows(
        table="transacoes_raw",
        rows=df_transacoes.values.tolist(),
        target_fields=df_transacoes.columns.tolist()
    )
    
    caminho_propostas = f"{caminho_raw}/tabelas_sql/propostas_credito.csv"
    df_propostas = pd.read_csv(caminho_propostas)
    hook_dw.insert_rows(
        table="propostas_credito_raw", 
        rows=df_propostas.values.tolist(),
        target_fields=df_propostas.columns.tolist()
    )
    
    caminho_contas = f"{caminho_raw}/tabelas_sql/contas.csv"
    df_contas = pd.read_csv(caminho_contas)
    hook_dw.insert_rows(
        table="contas_raw",
        rows=df_contas.values.tolist(),
        target_fields=df_contas.columns.tolist()
    )

with DAG(
    dag_id="desafio_bancovic",
    schedule="35 4 * * *",  
    start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
    catchup=False,
    tags=["bancovic", "etl"],
) as dag:
    extrair_csv_task = PythonOperator(
        task_id="extrair_csv_para_datalake",
        python_callable=extrair_csv_para_datalake
    )

    extrair_sql_task = PythonOperator(
        task_id="extrair_sql_para_datalake",
        python_callable=extrair_sql_para_datalake
    )
    
    carregar_dados_task = PythonOperator(
        task_id="carregar_para_data_warehouse",
        python_callable=carregar_para_data_warehouse
    )

    extrair_csv_task >> carregar_dados_task
    extrair_sql_task >> carregar_dados_task