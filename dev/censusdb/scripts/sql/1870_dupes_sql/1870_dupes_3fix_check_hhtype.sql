with
cte1 as (
select distinct histid, hhtype
from cr1870d_fix1
order by histid, hhtype),

cte2 as (
select histid
from cte1
group by histid
having count(*) = 1),

cte3 as (
select * from cte1 a
inner join cte2 b on a.histid = b.histid
order by a.histid, a.hhtype)

select * from cte3





/*
select * from cr1870d_fix1
where hhtype not in ('4')
order by famsize, year, histid_head, serial
*/
