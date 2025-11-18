-- get fixed dupe records by compound_key

with
cte1 as (
select distinct a.*
from cr1870_f1 a
left join cr1870_3fix3 b on a.composite_key = b.composite_key
where b.composite_key is not null)

select composite_key, count(*)
from cte1
group by composite_key
having count(*) > 1


