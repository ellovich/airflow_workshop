import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --------------------------------
# ------ требования дашорда ------
# --------------------------------

# -- SCD2 -- учебные планы (требования 1 и 2)
def my_up():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
    """
        -- берет только последнюю по времени запись для каждого constr_id (при условии что rn = 1)          
        WITH NewRecords AS (
            SELECT
                id,
                constr_id,
                isu_id,
                status,
                labor,
                (json_array_elements(dir_list::JSON)->>'year')::integer AS year,
                json_array_elements(dir_list::JSON)->>'qualification' AS qualification,
                update_ts,
                ROW_NUMBER() OVER (PARTITION BY constr_id ORDER BY update_ts DESC) AS rn
            FROM stg.op_dir
        )
        INSERT INTO dds.my_up (
            constr_id,
            isu_id,
            status,
            labor,
            year,
            qualification,
            update_ts
        )
        SELECT 
            nr.constr_id,
            nr.isu_id,
            nr.status,
            nr.labor,
            nr.year,
            nr.qualification,
            nr.update_ts
        FROM NewRecords nr	
            LEFT JOIN dds.my_up mu ON mu.constr_id = nr.constr_id
        WHERE 
            nr.rn = 1 
            AND (
                mu.constr_id IS NULL
                or mu.status != nr.status
                or mu.labor != nr.labor
            )
    """
    )

# -- SCD2 -- дисциплины (требования 3 и 4)
def disc():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
		WITH NewRecords AS (
            SELECT
                (json_array_elements(disc_list::JSON)->>'id')::integer AS constr_id
                ,(json_array_elements(dir_list::JSON)->>'id')::integer AS dir_constr_id
                ,(json_array_elements(dir_list::JSON)->>'ap_isu_id')::integer AS dir_isu_id
                ,(json_array_elements(disc_list::JSON)->>'discipline_code')::integer AS discipline_code
                ,(json_array_elements(disc_list::JSON)->>'status') AS status
                ,(json_array_elements(disc_list::JSON)->>'title') AS title
                ,update_ts
                ,ROW_NUMBER() OVER (PARTITION BY constr_id ORDER BY update_ts DESC) AS rn
            FROM stg.dir_disc
        )             
        INSERT INTO dds.disc (
            constr_id,
            dir_constr_id,
            dir_isu_id,
            status,
            title,
            update_ts
        )
        select distinct
            nr.constr_id,
            nr.dir_constr_id,
            nr.dir_isu_id,
            nr.status,
            nr.title,
            nr.update_ts
        FROM NewRecords nr 
        	LEFT JOIN dds.disc d ON d.constr_id = nr.constr_id
        WHERE 
        	nr.rn = 1 
            AND (
            	d.constr_id is null -- если записи не существовало
                or d.status != d.status
            )
        """
    )

# -- статусы (4ое требование дашборда)
def states():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.states RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        INSERT INTO dds.states (
            code, 
            name
        ) 
        VALUES
            ('AC', 'одобрено'),
            ('AR', 'архив'),
            ('EX', 'на экспертизе'),
            ('RE', 'на доработке')
        """
    )


# --------------------------
# ------ для разрезов ------
# --------------------------

# -- направление (1й разрез)
def dirs():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.dirs RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        insert into dds.dirs (
            constr_id
            ,isu_id
            ,selection_year
            ,title
        )
        select distinct 
            (json_array_elements(dir_list::JSON)->>'id')::integer AS constr_id
            ,(json_array_elements(dir_list::JSON)->>'ap_isu_id')::integer AS isu_id
            ,(json_array_elements(dir_list::JSON)->>'year')::integer AS selection_year
            ,(json_array_elements(dir_list::JSON)->>'title') AS title
        from stg.dir_disc dd 
        ON CONFLICT ON CONSTRAINT dirs_uindex 
        DO UPDATE 
        SET 
            constr_id = EXCLUDED.constr_id, 
            isu_id = EXCLUDED.isu_id,
            selection_year = EXCLUDED.selection_year, 
            title = EXCLUDED.title
        """
    )

# -- структурные подразделения для дисциплин (2й разрез)
def units():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.units RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        INSERT INTO dds.units (
            faculty_title, 
            faculty_id
        )
        SELECT DISTINCT 
            sw.faculty_title,
            sw.faculty_id
        FROM stg.unit_disc sw
        ON CONFLICT ON CONSTRAINT units_uindex 
        DO UPDATE 
        SET 
            faculty_title = EXCLUDED.faculty_title, 
            faculty_id = EXCLUDED.faculty_id
        """
    )

# -- уровень образования (3й разрез)
def levels():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.levels RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        INSERT INTO dds.levels (
            qualification,
            training_period
        ) 
        VALUES
            ('магистратура', 2),
            ('бакалавриат', 4),
            ('специалитет', 5);
        """
    )

# -- редакторы (5й разрез)
def editors ():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.editors RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.editors (id, username, first_name, last_name, email, isu_number)
    SELECT distinct
    	(json_array_elements(ud.disc_list ::json->'editors')::json->>'id')::integer AS editor_id, 
        (json_array_elements(ud.disc_list::json->'editors')::json->>'username') AS username,
        (json_array_elements(ud.disc_list::json->'editors')::json->>'first_name') AS first_name,
        (json_array_elements(ud.disc_list::json->'editors')::json->>'last_name') AS last_name,
        (json_array_elements(ud.disc_list::json->'editors')::json->>'email') AS email,
        (json_array_elements(ud.disc_list::json->'editors')::json->>'isu_number') AS isu_number
    FROM stg.unit_disc ud
    ON CONFLICT ON CONSTRAINT editors_uindex 
    DO UPDATE 
    SET 
        username = EXCLUDED.username, 
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name, 
        email = EXCLUDED.email, 
        isu_number = EXCLUDED.isu_number;
    """)

# # -- учебный план (6й разрез)
# def up():
#     PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
#         """
#         TRUNCATE dds.up RESTART IDENTITY CASCADE;
#         """
#     )
#     PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
#         """
#         INSERT INTO dds.up (
#             id, 
#             direction_id, 
#             edu_program_id, 
#             unit_id, 
#             level_id, 
#             selection_year
#         )
#         SELECT 
#             ud.id,
#             d.direction_id,
#             ud.edu_program_id::integer,
#             u.id AS unit_id,
#             l.id AS level_id,
#             ud.selection_year::integer
#         FROM stg.up_description ud
#             LEFT JOIN dds.directions d ON d.direction_code = ud.direction_code
#             LEFT JOIN dds.units u ON u.unit_title = ud.faculty_name
#             LEFT JOIN dds.levels l ON ud.training_period = l.training_period
#         """
#     )


with DAG(
    dag_id="stg_to_dds",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="0 4 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="my_up", python_callable=my_up)
    t2 = PythonOperator(task_id="disc", python_callable=disc)
    t3 = PythonOperator(task_id="states", python_callable=states)
    t4 = PythonOperator(task_id="dirs", python_callable=dirs)
    t5 = PythonOperator(task_id="units", python_callable=units)
    t6 = PythonOperator(task_id="levels", python_callable=levels)
    t7 = PythonOperator(task_id="editors", python_callable=editors)
    # t8 = PythonOperator(task_id="up", python_callable=up)

[t3, t4, t5, t6, t7] >> t1 >> t2
