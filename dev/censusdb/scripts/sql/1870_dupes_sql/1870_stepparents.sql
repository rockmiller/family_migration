drop table if exists cr1870_step;
create table cr1870_step as (
select year, histid, histid_sp, histid_pop,steppop, histid_mom, stepmom, histid_head, birthyr, relate
from cr1870
where stepmom not in ('0') or steppop not in ('0')
order by year, histid_head, birthyr);

select * from cr1870_step


