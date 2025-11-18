drop table if exists padl;
create table padl as (
select distinct on (hik) hik, sex
from pcpp
where rolecode in ('1','2')
order by hik, sex asc);

create index padl_hik on padl(hik);

