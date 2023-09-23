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
 
# авторизация
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



def get_dir_disc(): 
    postgres_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    target_fields = [
        "constr_id"
        ,"dir_list"
        ,"disc_list"
        ,"update_ts"
    ]

    url_down = "https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1" # первая страница
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)["count"]
    logging.info(f"Обнаружено {c} записей")

    for p in range(1, c // 10 + 2):
        url_down = "https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=" + str(p)
        page = requests.get(url_down, headers=headers)

        res = json.loads(page.text)["results"]
        for r in res:
            df = pd.DataFrame([r], columns=r.keys())
            df["academic_plan_in_field_of_study"] = df[~df["academic_plan_in_field_of_study"].isna()]["academic_plan_in_field_of_study"].apply(
                lambda st_dict: json.dumps(st_dict)
            )
            df["wp_in_academic_plan"] = df[~df["wp_in_academic_plan"].isna()]["wp_in_academic_plan"].apply(
                lambda st_dict: json.dumps(st_dict)
            )
            df.loc[:, "update_ts"] = pendulum.now().to_iso8601_string()

            # Проверяем, совпадают ли значения датафрейма с последней по времени записью с этим constr_id
            existing_data = postgres_hook.get_pandas_df(
                sql.SQL('''
                    WITH latest_data AS (
                        SELECT *
                        FROM stg.dir_disc
                        WHERE constr_id = {}
                        ORDER BY update_ts DESC
                        LIMIT 1
                    )
                    SELECT *
                    FROM latest_data ld
                    WHERE ld.constr_id = {}
                        AND ld.dir_list = {}
                        AND ld.disc_list = {}
                ''')
                .format(
                    sql.Literal(df['id'][0]),
                    sql.Literal(df['id'][0]),
                    sql.Literal(df['academic_plan_in_field_of_study'][0]),
                    sql.Literal(df['wp_in_academic_plan'][0]),
                )
            )
            if existing_data.empty:
                postgres_hook.insert_rows("stg.dir_disc", df.values, target_fields=target_fields)
                logging.info(f"Added id={df['id'][0]}")



def get_unit_disc():
    postgres_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    postgres_hook.run(
        """
        TRUNCATE dds.unit_disc RESTART IDENTITY CASCADE;
        """
    )
    target_fields = [
        "faculty_id" 
        ,"faculty_title" 
        ,"disc_list"
    ]

    url_down = "https://op.itmo.ru/api/record/structural/workprogram"
    page = requests.get(url_down, headers=headers)
    res = list(json.loads(page.text))

    for su in res:

        df = pd.DataFrame.from_dict(su)
        df["id"] = df["id"].values.astype(int)
        df["work_programs"] = df[~df["work_programs"].isna()]["work_programs"].apply(
            lambda st_dict: json.dumps(st_dict)
        )

        # Проверяем, есть ли такая запись
        existing_data = postgres_hook.get_pandas_df(
            sql.SQL('''
                SELECT *
                FROM stg.unit_disc
                WHERE faculty_id = {}
                    AND disc_list = {}
            ''')
            .format(
                sql.Literal(df['id'][0]),
                sql.Literal(df['work_programs'][0])
            )
        )
        if existing_data.empty:
            postgres_hook.insert_rows("stg.unit_disc", df.values, target_fields=target_fields)
            logging.info(f"Added fak_id={df['id'][0]}")



def get_op_dir():
    postgres_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    target_fields = [
        "constr_id"
        ,"isu_id"
        ,"status"
        ,"labor"
        ,"dir_list"
        ,"update_ts"
    ]

    op_constr_ids = postgres_hook.get_records(
        """
        SELECT constr_id AS op_constr_id
        FROM stg.dir_disc
        """
    )
    op_constr_ids = [int(result[0]) for result in op_constr_ids] # преобразование к целым числам

    def url(op_constr_id: int) -> str:
        return f"https://op.itmo.ru/api/academicplan/detail/{str(op_constr_id)}?format=json"

    for op_id in op_constr_ids:
        page = requests.get(url(op_id), headers=headers)

        df = pd.DataFrame.from_dict(page.json(), orient="index")
        df = df.T
        df["academic_plan_in_field_of_study"] = df[~df["academic_plan_in_field_of_study"].isna()]["academic_plan_in_field_of_study"].apply(
            lambda st_dict: json.dumps(st_dict)
        )
        df["update_ts"] = pendulum.now().to_iso8601_string()    

        # лучше бы не переименовывать на этом слое, но так мне удобнее :)
        df = df.rename(columns={
            "id": "constr_id"
            ,"ap_isu_id": "isu_id"
            ,"on_check": "status"
            ,"laboriousness": "labor"
            ,"academic_plan_in_field_of_study": "dir_list"
        })
        df = df[target_fields]

        # Проверяем, существует ли запись с такими же данными
        existing_data = postgres_hook.get_pandas_df(
            sql.SQL('''
                WITH t AS (
                    SELECT *
                    FROM stg.op_dir
                    WHERE constr_id = {}
                    ORDER BY update_ts DESC
                    LIMIT 1
                )
                SELECT *
                FROM t
                WHERE constr_id = {}
                	AND labor = {}
                	AND status = {}
                    AND dir_list = {}
            ''')
            .format(
                sql.Literal(df['constr_id'][0])
                ,sql.Literal(df['constr_id'][0])
                ,sql.Literal(df['labor'][0])
                ,sql.Literal(df['status'][0])
                ,sql.Literal(df['dir_list'][0])
            )
        )

        if existing_data.empty:
            postgres_hook.insert_rows("stg.op_dir", df.values, target_fields=target_fields)
            logging.info(f"Added constr_id={df['constr_id'][0]}")


def get_disc_annotation():
    postgres_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    target_fields = [
        "constr_id" 
        ,"title"
        ,"discipline_code" 
        ,"prerequisites" 
        ,"outcomes"
        ,"update_ts"
    ]

    disc_ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
        """
        select distinct json_array_elements(disc_list::json)->>'discipline_code' as discipline_code
        from stg.dir_disc
        order by discipline_code
        """)
    disc_ids = [int(id[0]) for id in disc_ids if id[0] is not None]

    def url(disc_id: int) -> str:
        return f"https://op.itmo.ru/api/workprogram/items_isu/{disc_id}?format=json"

    for disc_id in disc_ids:
        page = requests.get(url(disc_id), headers=headers)

        df = pd.DataFrame.from_dict(page.json(), orient="index")
        df = df.T
        df["prerequisites"] = df[~df["prerequisites"].isna()]["prerequisites"].apply(
            lambda st_dict: json.dumps(st_dict)
        )
        df["outcomes"] = df[~df["outcomes"].isna()]["outcomes"].apply(
            lambda st_dict: json.dumps(st_dict)
        )
        df["update_ts"] = pendulum.now().to_iso8601_string()    


        # Проверяем, существует ли запись с такими же данными
        existing_data = postgres_hook.get_pandas_df(
            sql.SQL('''
                WITH t AS (
                    SELECT *
                    FROM stg.disc_annot
                    WHERE discipline_code = {}
                    ORDER BY update_ts DESC
                    LIMIT 1
                )
                SELECT *
                FROM t
                WHERE discipline_code = {}
                	AND prerequisites = {}
                	AND outcomes = {}
            ''')
            .format(
                sql.Literal(df['discipline_code'][0])
                ,sql.Literal(df['discipline_code'][0])
                ,sql.Literal(df['prerequisites'][0])
                ,sql.Literal(df['outcomes'][0])
            )
        )

        if existing_data.empty:
            logging.info(f"Added discipline_code={df['id'][0]}")
            postgres_hook.insert_rows("stg.disc_annot", df.values, target_fields=target_fields)



with DAG(
    dag_id="get_data",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_dir_disc", python_callable=get_dir_disc)   # Какие дисциплины есть на направлении
    t2 = PythonOperator(task_id="get_unit_disc", python_callable=get_unit_disc) # Структурные подразделения (факультеты) и дисциплины, которые они реализуют
    t3 = PythonOperator(task_id="get_op_dir", python_callable=get_op_dir)       # Образовательная программа и направления
    t4 = PythonOperator(task_id="get_disc_annotation", python_callable=get_disc_annotation) # Аннотации к дисциплинам


[t1, t2] >> t3 >> t4