drop table if exists cr1870_3fix1;
create table cr1870_3fix1 as (

-- get list of unique histids for duplicated records

with 
cte1 as (
select distinct histid
from cr1870_dupes
group by histid
having count(*) > 1),

-- get the standard set of columns

cte2 as (
select distinct a.year, a.serial, a.histid, 
	a.hik, a.sex, a.birthyr::integer as birthyr,
	a.hhtype, a.famsize::integer as famsize,
	a.histid_head, a.relate, a.histid_sp, a.histid_pop, a.histid_mom, a.steppop, a.stepmom
from cr1870_dupes a
inner join cte1 b on a.histid = b.histid)

select * from cte2
order by year, serial, histid);








































