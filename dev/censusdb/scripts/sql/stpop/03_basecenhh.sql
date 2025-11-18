-- select base population
-- step 2: get households corresponding to base links
-- union all cr_yyyy

drop table if exists basecenhh;
create table basecenhh as (

select histid, pernum, birthyr, sex, histid_head, relate, related, histid_sp, histid_pop, histid_mom, stepmom, steppop
from cr1850 a
inner join basehiks b on a.year = b.year and a.serial = b.serial

union

select histid, pernum, birthyr, sex, histid_head, relate, related, histid_sp, histid_pop, histid_mom, stepmom, steppop
from cr1860 a
inner join basehiks b on a.year = b.year and a.serial = b.serial

union

select histid, pernum, birthyr, sex, histid_head, relate, related, histid_sp, histid_pop, histid_mom, stepmom, steppop
from cr1870 a
inner join basehiks b on a.year = b.year and a.serial = b.serial

union

select histid, pernum, birthyr, sex, histid_head, relate, related, histid_sp, histid_pop, histid_mom, stepmom, steppop
from cr1880 a
inner join basehiks b on a.year = b.year and a.serial = b.serial
order by year, serial, pernum);

create index ix_basecenhh_hik on basecenhh(hik);
create index ix_basecenhh_hik on basecenhh(year, serial);
