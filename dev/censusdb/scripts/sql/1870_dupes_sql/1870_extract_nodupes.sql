drop table if exists cr1870_nodupes;
create table cr1870_nodupes as (
with
dupe_histids as (
select distinct histid from cr1870_dupes)

select a.* 
from cr1870 a
left join dupe_histids b on a.histid = b.histid
where b.histid is null
);