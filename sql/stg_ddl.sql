-- endpoint 1: https://op.itmo.ru/api/record/structural/workprogram
-- Структурные подразделения (факультеты) и дисциплины, которые они реализуют (SCD0)
create table stg.unit_disc (
    faculty_id integer 
    ,faculty_title text 
    ,disc_list text
);
ALTER TABLE stg.unit_disc ADD CONSTRAINT unit_disc_uindex UNIQUE (faculty_id);

/*
disc_list:
{
    "id": 17969,
    "title": "Название дисциплины",
    "discipline_code": "21201",
    "editors": [
        {
            "id": 1277,
            "username": "313131",
            "first_name": "Иван",
            "last_name": "Иванов",
            "email": "example@gmail.com",
            "isu_number": "313131"
        }
    ]
}
*/

-- endpoint 2: https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all
-- Показывает какие дисциплины есть на направлении
--  - из направления берется год набора
--  - из дисциплин берется статус (его динамику надо отслеживать)
create table stg.dir_disc (
    id serial            -- локальный id
    ,constr_id integer   -- id направления в "Конструктор ОП"
    ,dir_list text       -- направление
    ,disc_list text      -- список дисциплин, которые есть на направлении
    ,update_ts timestamp -- SCD2
);
ALTER TABLE stg.dir_disc ADD CONSTRAINT dir_disc_uindex UNIQUE (id);
/*
dir_list: в каждом таком списке только одно направление
[
    {
        "id": 6859,
        "ap_isu_id": 10572,
        "year": 2018,
        "title": "Нанофотоника и квантовая оптика"
    }
]

disc_list:
[
    {
        "id": 2623,
        "discipline_code": "5546",
        "title": "История",
        "description": null,
        "status": "WK"
    },
    {
        "id": 2625,
        "discipline_code": "5664",
        "title": "Физическая культура",
        "description": null,
        "status": "WK"
    }
]
*/

-- endpoint 4: https://op.itmo.ru/api/academicplan/detail/{op_id}?format=json
-- образовательная программа и направления
-- - берется статус и трудоёмкость образовательной программы (их динамика отслеживается)
-- - из направления возьмется год набора и уровень образования
create table stg.op_dir (
    id serial            -- локальный id
    ,constr_id int       -- id образовательной программы (ОП) в "Конструктор ОП"
    ,isu_id int          -- id ОП в ИСУ
    ,status text         -- статус
    ,labor int           -- трудоемкость
    ,dir_list text       -- направление, вложенное в json-массив, содержит год набора и уровень образования
    ,update_ts timestamp -- SCD2
);
ALTER TABLE stg.op_dir ADD CONSTRAINT op_dir_uindex UNIQUE (id);

/*
dir_list:
[
    {
        "id": 6859,
        "year": 2018,
        "qualification": "bachelor",
        "title": "Нанофотоника и квантовая оптика",
        "field_of_study": [
            {
                "number": "16.03.01",
                "id": 15772,
                "title": "Техническая физика",
                "qualification": "bachelor",
                "educational_profile": null,
                "faculty": null
            }
        ],
        "plan_type": "base",
        "training_period": 0,
        "structural_unit": null,
        "total_intensity": 0,
        "military_department": false,
        "university_partner": [],
        "editors": []
    }
]
*/


-- аннотация дисциплины
create table stg.disc_annot (
    id serial
    ,constr_id integer 
    ,title text
    ,discipline_code integer 
    ,prerequisites text 
    ,outcomes text
    ,update_ts timestamp
);
ALTER TABLE stg.disc_annot ADD CONSTRAINT disc_annot_uindex UNIQUE (id);

-- дисциплины по годам
create table stg.disc_by_year
(id integer, ap_isu_id integer, title text, work_programs text);