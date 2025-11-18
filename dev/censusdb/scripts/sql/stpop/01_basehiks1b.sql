-- select base population
-- step 1: get all links for the base geographic and categorical criteria
-- union results from all cr_y tables
-- index by hik and by year-serial

drop table if exists basehiks;
create table basehiks as (
select distinct hik, year, serial
from cr1850
where stateicp in ('51') and hik is not null and year in ('1850')
union
select distinct hik, year, serial
from cr1860
where stateicp in ('51') and hik is not null and year in ('1850')
union
select distinct hik, year, serial
from cr1870
where stateicp in ('51') and hik is not null and year in ('1850')
union
select distinct hik, year, serial
from cr1880
where stateicp in ('51') and hik is not null and year in ('1850')
order by hik, year, serial);

create index ix_basehiks_hik on basehiks(hik);
create index ix_basehiks_ser on basehiks(year, serial);



