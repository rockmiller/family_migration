-- get stepfather records

create table cper_step as (
select year, serial, histid, relate, histid_pop, histid_mom, steppop, stepmom
from cper1
where steppop in ('1') or stepmom in ('1')
order by year, serial, histid);
