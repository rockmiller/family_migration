drop table if exists cr1870_dupes_rows;
create table cr1870_dupes_rows as (

with cte1 as (
select histid, count(*)
from cr1870 a
group by histid
having count(*) > 1),

cte2 as (
select distinct 
	b.*,
	COALESCE(b.histid, repeat('$', length(b.histid))) ||
	COALESCE(b.histid_head, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_sp, repeat('$',length(b.histid))) ||
  	COALESCE(b.histid_pop, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_mom, repeat('$', length(b.histid))) AS composite_key
from cr1870 b
inner join cte1 a on a.histid = b.histid
order by year, serial, histid)

select a.*,
	row_number() over (order by year, serial, histid) as row
from cte2 a

);
