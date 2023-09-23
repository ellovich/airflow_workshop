-- количество дисциплин без разметки
select count (distinct discipline_code)
from dds.wp_markup wm left join dds.wp_up wu on wu.wp_id = wm.id 
where (prerequisites = '[]' or outcomes = '[]')
    and wu.up_id in (select id from dds.up u where u.selection_year = '2023')
