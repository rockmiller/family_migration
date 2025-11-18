-- get stepfather records

create table tmp_step as (
select year, serial, histid, relate, histid_pop, histid_mom, steppop, stepmom
from cr1870
where steppop in ('1') or stepmom in ('1')
order by year, serial, histid)
union
select year, serial, histid, relate, histid_pop, histid_mom, steppop, stepmom
from cpstep;
