drop table if exists studyhiks1;
create table studyhiks1 as (
with
cte1 as (
select distinct year, serial 
from basehiks)

select distinct a.hik, a.year, a.serial, a.histid, a.stateicp, a.countyicp
from cr1850 a
inner join basehiks b on a.year = b.year and a.serial = b.serial
where a.hik is not null
order by hik, year, serial);




