with
y1 as (
select distinct hik, year, serial
from cr1850),

y2 as (
select distinct a.hik, a.year as year1, a.serial as serial1, b.year as year2, b.serial as serial2
from y1 a
inner join y1 b on a.hik = b.hik
where a.year != b.year and a.serial != b.serial
)
select * from y2

