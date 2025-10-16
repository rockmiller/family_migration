-- definition of base population: families in KB in 1870
-- adults (rolecode 1,2) and children (rolecode 3 both)
-- in Bluegrass (zone KB)

drop table if exists bpop;
create table bpop as (
with

bp as (
select distinct a.hik, a.sex
from pcpp as a
inner join icp as b on b.countycode = a.countycode
where cenyear in (1870)
and zone in ('KB')),

bp2 as (
select distinct on (h.hik) h.hik, p.sex, p.race, p.birthyr, p.bpl, p.farm,
		case when s.slvhcat is null then '0' else s.slvhcat end as slvhcat
from pcpp as p
inner join bp as h on p.hik = h.hik
left join slv as s on s.hik = h.hik
order by hik, birthyr, farm, sex, race)

select * from bp2);

create index bpop_hik on bpop(hik);

