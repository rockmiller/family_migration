drop table if exists cr1870_3fix2;
create table cr1870_3fix2 as (

with
cte1 as (
select distinct histid, hhtype
from cr1870_3fix1
order by histid, hhtype),

cte2 as (
select histid
from cte1
group by histid
having count(*) = 1),

cte3 as (
select a.* from cte1 a
inner join cte2 b on a.histid = b.histid
order by a.histid, a.hhtype),

cte4 as (
select a.* from cr1870_3fix1 a
inner join cte3 b on a.histid = b.histid
),

cte5 as (
select b.*, 
	COALESCE(b.histid, repeat('$', length(b.histid))) ||
	COALESCE(b.histid_head, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_sp, repeat('$',length(b.histid))) ||
  	COALESCE(b.histid_pop, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_mom, repeat('$', length(b.histid))) AS composite_key
from cte4 b
)
select * from cte5
order by year, serial, histid
);

