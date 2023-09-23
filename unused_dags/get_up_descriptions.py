import json

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

url_auth = "https://id.itmo.ru/auth/realms/itmo/protocol/openid-connect/token"
auth_data = {
    "client_id": Variable.get("client_id"),
    "client_secret": Variable.get("client_secret"),
    "grant_type": Variable.get("grant_type"),
}


def get_up(up_id):
    up_id = str(up_id)
    url = "https://disc.itmo.su/api/v1/academic_plans/" + up_id
    token_txt = requests.post(url_auth, auth_data).text
    token = json.loads(token_txt)["access_token"]
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Token " + token,
    }
    page = requests.get(url, headers=headers)
    df = pd.DataFrame(page.json()["result"])
    df = df.drop(["disciplines_blocks"], axis=1)
    if len(df) > 0:
        PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").insert_rows(
            "stg.up_description",
            df.values,
            target_fields=df.columns.tolist(),
            replace=True,
            replace_index="id",
        )


def get_up_description():
    ids = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
        SELECT (json_array_elements(academic_plan_in_field_of_study::JSON)->>'ap_isu_id')::integer AS ap_isu_id
        FROM stg.work_programs wp
        ORDER BY 1
        """
    )
    start = 0
    finish = start + 100
    while start < len(ids):
        token_txt = requests.post(url_auth, auth_data).text
        token = json.loads(token_txt)["access_token"]
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Token " + token,
        }

        if finish > len(ids):
            finish = len(ids) + 1
        for up_id in ids[start:finish]:
            up_id = str(up_id[0])
            url = "https://disc.itmo.su/api/v1/academic_plans/" + up_id
            print(url)
            page = requests.get(url, headers=headers)
            print(page)
            df = pd.DataFrame(page.json()["result"])
            df = df.drop(["disciplines_blocks"], axis=1)
            if len(df) > 0:
                PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").insert_rows(
                    "stg.up_description",
                    df.values,
                    target_fields=df.columns.tolist(),
                    replace=True,
                    replace_index="id",
                )
        start += 100
        finish = start + 100


with DAG(
    dag_id="get_up_descriptions",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="0 5 * * 0",
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="get_up_description", python_callable=get_up_description
    )


t1
