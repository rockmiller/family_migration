drop table if exists cr1870_3fix3;
create table cr1870_3fix3 as (

-- get distinct '4' histids (solo male)

with
cte0 as (
select b.*
from cr1870_3fix2 b
),


cte1 as (
select distinct histid from cte0
where hhtype in ('4')
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
select distinct a.* from cte0 a
inner join cte3 b on a.histid = b.histid
order by a.histid
),

-- get fixed records with duplicates

cte6 as (
select distinct a.* from cte0 a
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
select distinct * from cte6 a
where row(histid_sp, histid_pop, histid_mom) is null
order by a.histid
),

cte9 as (
select distinct a.* 
from cte0 a
inner join cte7 b on a.histid = b.histid
),

-- get null histids that are not in non-null histids

cte10 as (
select distinct a.histid 
from cte7 a
left join cte8 b on a.histid = b.histid
where b.histid is null
order by a.histid
),

--merging the separate spouse/parent records

cte11 as (
select distinct
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
	histid_mom, steppop, stepmom, histid_head,
	first_value(composite_key) over (partition by histid order by histid_sp asc) as composite_key
from cte12),

-- filter the duplicates

cte14 as (
select b.*
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

select * from cte15a
);
create index idx_3fix3_composite_key on cr1870_3fix3(composite_key);





















