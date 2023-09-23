import json

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

url = "https://op.itmo.ru/auth/token/login"
auth_data = {
    "username": Variable.get("username"), 
    "password": Variable.get("password")
}
token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {
    "Content-Type": "application/json", 
    "Authorization": "Token " + token
}


def get_evaluation_tools():
    postgres_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    postgres_hook.run("TRUNCATE stg.evaluation_tools RESTART IDENTITY CASCADE;")

    url_down = "https://op.itmo.ru/api/tools/?page=1"
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)["count"]

    for p in range(1, c // 10 + 2):
        url_down = "https://op.itmo.ru/api/tools/?page=" + str(p)
        page = requests.get(url_down, headers=headers)
        res = json.loads(page.text)["results"]

        for r in res:
            df = pd.DataFrame([r], columns=r.keys())

            postgres_hook.insert_rows(
                "stg.evaluation_tools",
                df.values,
                target_fields=df.columns.tolist(),
                replace=True,
                replace_index="id",
            )



with DAG(
    dag_id="get_tools",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="get_evaluation_tools", python_callable=get_evaluation_tools
    )


t1
