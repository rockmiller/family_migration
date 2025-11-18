drop table if exists pchl;
create table pchl as (
with 

roles as (
select distinct on (hik) hik, rolecode, cenyear, sex
from pcpp
order by hik, rolecode asc, cenyear desc),

children as (
select * from roles
where rolecode in ('3'))

select * from children);

create index pchl_hik on pchl(hik);

