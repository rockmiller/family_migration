

-- process separate sp / mom / pop / blank records

cte3s as (
select distinct a.year, a.serial, a.histid_head, a.birthyr, a.sex, a.histid, a.histid_sp, a.histid_pop, a.histid_mom, a.hik
from cte2 a
left join cte1 b on a.histid = b.histid
where a.histid_sp is null and row(a.histid_pop,a.histid_mom) is not null
order by year, serial, birthyr, sex)

select * from cte3s

-- spouse records

cte9s as (
select distinct a.year, a.serial, a.histid_head, a.birthyr, a.sex, a.histid, a.histid_sp, a.histid_pop, a.histid_mom, a.hik
from cte2 a
join cte6a b on a.histid = b.histid
where histid_sp is null and row(histid_pop,histid_mom) is not null
order by year, serial, birthyr, sex),

-- parent records

cte9p as (
select distinct a.year, a.serial, a.histid_head, a.birthyr, a.sex, a.histid, a.histid_sp, a.histid_pop, a.histid_mom, a.hik
from cte2 a
join cte6a b on a.histid = b.histid
where histid_sp is not null
order by year, serial, birthyr, sex),

-- no-family records

cte9nofam as (
select distinct a.year, a.serial, a.histid_head, a.birthyr, a.sex, a.histid, a.histid_sp, a.histid_pop, a.histid_mom, a.hik
from cte6 a
where row (histid_sp, histid_pop, histid_mom) is null
order by year, serial, birthyr, sex),

cte9u as (
select * from cte9p union all select * from cte9s),

cte9bh as (
select distinct a.histid
from cte9u a
left join cte9nofam b on a.histid = b.histid
where b.histid is null
order by histid),

-- non-family histids

cte10nf as (
select histid from cte9nofam),

-- family histids

cte10f as (
select histid from cte9u),

-- fixing errors where there are simultaneous spouse and mom/pop records

cte10fixhh as (
select distinct year, serial
from cte6 a
inner join (
select distinct histid, count(*)
from cte10f
group by histid
having count(*) > 1) b on a.histid = b.histid),

cte10fix as (
select distinct a.year, a.serial, a.histid_head, a.birthyr, a.sex, a.histid, a.histid_sp, a.histid_pop, a.histid_mom, a.hik
from cte6 a
inner join cte10fixhh b on a.serial = b.serial
where relate in ('2')
order by a.serial, a.birthyr, a.sex),

-- final sets: family (excluding fix) + family (fix) + non-family (remaining)

cte12fix as (
select distinct histid
from cte10fix),

cte12f as (
select distinct a.*
from cte9u a
left join cte12fix b on a.histid = b.histid
where b.histid is null
order by histid),

fx1870 as (
select distinct a.year, a.serial, a.histid_head, a.birthyr, a.sex, a.histid, a.histid_sp, a.histid_pop, a.histid_mom, a.hik
from cte12f a
union all
select distinct a.year, a.serial, a.histid_head, a.birthyr, a.sex, a.histid, a.histid_sp, a.histid_pop, a.histid_mom, a.hik
from cte10fix a)

-- assign extraction keys and make a temporary table

SELECT
	a.*,
	COALESCE(a.histid, repeat('$', length(histid))) ||
	COALESCE(a.histid_head, repeat('$', length(histid))) ||
  	COALESCE(a.histid_sp, repeat('$',length(histid))) ||
  	COALESCE(a.histid_pop, repeat('$', length(histid))) ||
  	COALESCE(a.histid_mom, repeat('$', length(histid))) AS composite_key,
	1 as fixkey
FROM fx1870 a
);