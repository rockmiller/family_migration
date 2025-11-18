-- definition of base population
-- adults (rolecode 1,2)
-- in 1870,1880
-- in Bluegrass

with

bp as (
select distinct a.hik, a.sex
from pcpp as a
inner join icp as b on b.countycode = a.countycode
where rolecode in ('1','2') and cenyear in (1870,1880)
and zone in ('KB')),

bp2 as (
select distinct on (h.hik) h.hik, sex, race, birthyr, bpl, farm,
		case when s.slvhcat is null then '0' else s.slvhcat end as slvhcat
from pcpp as p
inner join bp as h on p.hik = h.hik
left join slv as s on s.hik = h.hik
order by hik, birthyr, farm, sex, race)

select * from bp2

