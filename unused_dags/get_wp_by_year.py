import json
import logging
import warnings
import numpy as np
import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

# для использования int64 в sql
from psycopg2.extensions import AsIs, register_adapter
register_adapter(np.int64, AsIs)

# отключить предупреждения от pandas
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")


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


# 3) рабочие программы по годам (таблица wp_by_year)
def get_wp_by_year():
    postgres_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    target_fields = [
        "id",
        "ap_isu_id", 
        "title", 
        "work_programs", 
        "update_ts"
    ]

    # идентификаторы учебных планов
    up_ids = postgres_hook.get_records(
        '''
        WITH t AS (
            SELECT (json_array_elements(academic_plan_in_field_of_study::JSON)->>'ap_isu_id') :: integer AS isu_id, id
            FROM stg.work_programs wp
            WHERE id < 7256
        )
        SELECT id
        FROM t
        ORDER BY id
        '''
    )
    up_ids = [int(result[0]) for result in up_ids] # преобразование к целым числам

    for up_id in up_ids:
        url = "https://op.itmo.ru/api/record/academicplan/get_wp_by_year/" + str(up_id) + "?year=2023/2024"
        page = requests.get(url, headers=headers)

        df = pd.DataFrame.from_dict(page.json())
        df["work_programs"] = df[~df["work_programs"].isna()]["work_programs"].apply(
            lambda st_dict: json.dumps(st_dict)
        )
        df["update_ts"] = pendulum.now().to_iso8601_string()

        if len(df) > 0:
            # Проверяем, существует ли запись с такими же данными
            existing_data = postgres_hook.get_pandas_df(
                sql.SQL('''
                    SELECT *
                    FROM stg.wp_by_year
                    WHERE id = {}
                        AND ap_isu_id = {} 
                        AND title = {}
                        AND work_programs = {}
                ''')
                .format(
                    sql.Literal(df['id'][0]),
                    sql.Literal(df['ap_isu_id'][0]),
                    sql.Literal(df['title'][0]),
                    sql.Literal(df['work_programs'][0])
                )
            )
    
            if existing_data.empty:
                postgres_hook.insert_rows("stg.wp_by_year", df.values, target_fields=target_fields)
                logging.info(f"Added id={df['id'][0]}")



with DAG(
    dag_id="get_wp_by_year",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    t3 = PythonOperator(task_id="get_wp_by_year", python_callable=get_wp_by_year)
