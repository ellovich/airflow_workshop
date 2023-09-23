--------------------------------
------ требования дашорда ------
--------------------------------

-- SCD2 -- учебные планы (требования 1 и 2)
CREATE TABLE dds.my_up (
    id serial
    ,constr_id int
    ,isu_id int
    ,status text
    ,labor integer 
    ,year int
    ,qualification text
    ,update_ts timestamp
);
ALTER TABLE dds.my_up ADD CONSTRAINT my_up_uindex UNIQUE (id);

-- SCD2 -- дисциплины (требования 3 и 4)
CREATE TABLE dds.disc (
	id serial
    ,constr_id int
    ,dir_constr_id int
    ,dir_isu_id int
    ,status text
    ,title text
    ,update_ts timestamp
);
ALTER TABLE dds.disc ADD CONSTRAINT disc_uindex UNIQUE (id);

-- статусы (4ое требование дашборда)
CREATE TABLE dds.states (
    code varchar(2)   -- код статуса: АС / WK / ...
    ,name varchar(20) -- расшифровка: одобрено / в работе / ...
);
ALTER TABLE dds.states ADD CONSTRAINT states_uindex UNIQUE (code, name);


--------------------------
------ для разрезов ------
--------------------------

-- направление (1й разрез)
CREATE TABLE dds.dirs (
    constr_id int         -- код из "Конструктор ОП"
    ,isu_id int           -- код из ИСУ
    ,selection_year int   -- год набора
    ,title text           -- например: Прикладная оптика
);
ALTER TABLE dds.dirs ADD CONSTRAINT dirs_uindex UNIQUE (constr_id, selection_year);

-- структурные подразделения для дисциплин (2й разрез)
CREATE TABLE dds.units (
    unit_title varchar(100) 
    ,faculty_id integer
);
ALTER TABLE dds.units ADD CONSTRAINT units_uindex UNIQUE (faculty_id, unit_title);

-- уровень образования (3й разрез)
CREATE TABLE dds.levels (
    id serial
    ,training_period varchar(5) 
    ,qualification varchar(20)
);
ALTER TABLE dds.levels ADD CONSTRAINT level_uindex UNIQUE (training_period, qualification);

-- редакторы (5й разрез)
CREATE TABLE dds.editors (
    id integer
    ,username varchar(50)
    ,first_name varchar(50)
    ,last_name varchar(50)
    ,email varchar(50)
    ,isu_number varchar(6)
);
ALTER TABLE dds.editors ADD CONSTRAINT editors_uindex UNIQUE (id);

-- -- учебный план (6й разрез)
-- CREATE TABLE dds.up (
--     id integer 
--     ,direction_id integer    -- направление
--     ,edu_program_id integer  -- образовательная программа
--     ,unit_id integer         -- структурное подразделение
--     ,level_id integer        -- уровень образование
--     ,selection_year integer  -- год набора
-- );
-- ALTER TABLE dds.up ADD CONSTRAINT up_uindex UNIQUE (id);
