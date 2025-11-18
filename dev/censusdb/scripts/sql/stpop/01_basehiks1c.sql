-- select base population
-- step 1: get all links for the base geographic and categorical criteria
-- union results from all cr_y tables
-- index by hik and by year-serial
-- see if where criteria can be packaged

drop table if exists basehiks;
create table basehiks as (
select hik
from cr1850
where stateicp in ('51') and hik is not null and year in ('1870')
union
select hik
from cr1860
where stateicp in ('51') and hik is not null and year in ('1870')
union
select hik
from cr1870
where stateicp in ('51') and hik is not null and year in ('1870')
union
select hik
from cr1880
where stateicp in ('51') and hik is not null and year in ('1870')
order by hik);

drop index if exists ix_basehiks_hik;
create index ix_basehiks_hik on basehiks(hik);



