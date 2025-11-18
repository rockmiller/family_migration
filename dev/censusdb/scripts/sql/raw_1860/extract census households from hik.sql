with
cte1 as (
select distinct year, serial
from cr1850
where hik in ('ayALLvUykE-2ajbjz26eE')
order by year, serial),

cte2 as (
select a.year, a.serial, a.hik, a.pernum::integer, a.histid, a.sex, a.birthyr::integer, 
	relate::integer, related::integer, a.histid_sp, a.histid_pop, a.histid_mom, steppop, stepmom
from cr1850 a
inner join cte1 b on a.year = b.year and a.serial = b.serial
order by a.year, a.serial, a.pernum::integer, related::integer, birthyr::integer)

select * from cte2 where relate::integer in (1,2,3)

