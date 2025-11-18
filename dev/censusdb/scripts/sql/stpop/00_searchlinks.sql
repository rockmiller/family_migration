drop table if exists cs1850;
create table cs1850 as (
select distinct hik, year, serial, COALESCE(stateicp, '') || COALESCE(countyicp, '') AS loc
from cr1850
where hik is not null);

create table cs1860 as (
select distinct hik, year, serial, COALESCE(stateicp, '') || COALESCE(countyicp, '') AS loc
from cr1860
where hik is not null
union
select hik, year, serial, loc from cs1850
);

drop table if exists cs1850;

create table cs1870 as (
select distinct hik, year, serial, COALESCE(stateicp, '') || COALESCE(countyicp, '') AS loc
from cr1870
where hik is not null
union
select hik, year, serial, loc from cs1860
);

drop table if exists cs1860;

drop table if exists cs1880;
create table cs1880 as (
select distinct hik, year, serial, COALESCE(stateicp, '') || COALESCE(countyicp, '') AS loc
from cr1880
where hik is not null
union
select hik, year, serial, loc from cs1870
);

drop table if exists cs1870;

select * from cs1880 limit 1000








--drop index if exists ix_search_hik;
--drop index if exists ix_search_ys;

--create index ix_search_hik on searchlinks(hik);
--create index ix_search_ys on searchlinks(year, serial);



