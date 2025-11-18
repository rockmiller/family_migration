drop table if exists cr1870_3fix3;
create table cr1870_3fix3 as (

-- get distinct '4' histids (solo male)

with
cte0 as (
select b.*, 
	COALESCE(b.histid, repeat('$', length(b.histid))) ||
	COALESCE(b.histid_head, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_sp, repeat('$',length(b.histid))) ||
  	COALESCE(b.histid_pop, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_mom, repeat('$', length(b.histid))) AS composite_key,
	1 as rec
from cr1870_3fix2 b
),


cte1 as (
select distinct histid from cte0
where hhtype in ('4')
--and row(histid_sp, histid_pop, histid_mom) is null
),

-- histids not in 4

cte2 as (
select distinct histid from cte0
where hhtype not in ('4')
),
--and not row(histid_sp, histid_pop, histid_mom) is null

-- non-duplicated cte2 histids

cte3 as (
select a.histid from cte0 a
inner join cte2 b on a.histid = b.histid
group by a.histid
having count(*) = 1
),

-- duplicated cte2 histids

cte4 as (
select a.histid from cte0 a
inner join cte2 b on a.histid = b.histid
group by a.histid
having count(*) > 1
),

-- get fixed records non-duplicated

cte5 as (
select a.* from cte0 a
inner join cte3 b on a.histid = b.histid
order by a.histid
),

-- get fixed records with duplicates

cte6 as (
select a.* from cte0 a
inner join cte4 b on a.histid = b.histid
order by a.histid
),

-- get histids of non-null rows from cte6

cte7 as (
select distinct histid from cte6 a
where not row(histid_sp, histid_pop, histid_mom) is null
order by a.histid
),

-- get histids of null rows from cte6

cte8 as (
select * from cte6 a
where row(histid_sp, histid_pop, histid_mom) is null
order by a.histid
),

cte9 as (
select a.* 
from cte0 a
inner join cte7 b on a.histid = b.histid
),

-- get null histids that are not in non-null histids

cte10 as (
select a.histid 
from cte7 a
left join cte8 b on a.histid = b.histid
where b.histid is null
order by a.histid
),

--merging the separate spouse/parent records

cte11 as (
select 
	year, serial, b.histid, hik, sex, birthyr, hhtype, famsize, 
	histid_head, relate, histid_sp, histid_pop, histid_mom, steppop, stepmom, composite_key
from cte10 a
inner join cte0 b on a.histid = b.histid
order by b.histid
),

-- filter out the stepparent records

cte12 as (
select distinct year, serial, birthyr, sex, histid, histid_sp, histid_pop, histid_mom, 
	steppop, stepmom, histid_head, composite_key
from cte11
where stepmom in ('0') and steppop in ('0')
),

-- merge the spouse and parent records

cte13 as (
select distinct
	year, serial, birthyr, sex, histid, 
	first_value (histid_sp) over (partition by histid order by histid_sp asc) as histid_sp,
	first_value (histid_pop) over (partition by histid order by histid_sp asc) as histid_pop,
	histid_mom, steppop, stepmom, histid_head 
from cte12),

-- filter the duplicates

cte14 as (
select b.*, 
	COALESCE(b.histid, repeat('$', length(b.histid))) ||
	COALESCE(b.histid_head, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_sp, repeat('$',length(b.histid))) ||
  	COALESCE(b.histid_pop, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_mom, repeat('$', length(b.histid))) AS composite_key
from cte13 b
where not row(histid_pop, histid_mom) is null
order by histid
),

cte14b as (
select distinct histid from cte14),

-- combine fixed records with composite keys (for extraction from cr1870)

cte15a as (
select distinct year, serial, histid, composite_key from cte5
union all
select distinct year, serial, histid, composite_key from cte8
union all
select distinct year, serial, histid, composite_key from cte14
)

select *, row_number() over (order by year, composite_key) as row from cte15a
);

create index idx_1870fix3_composite on cr1870_3fix3 (composite_key);





/*
-- (to inspect a household)

cte13hh as (
select year, a.serial, birthyr, sex, histid, histid_sp, histid_pop, histid_mom, 
	steppop, stepmom
from cr1870 a
inner join (select distinct serial from cte12) b
	on a.serial = b.serial
order by serial)
*/

















