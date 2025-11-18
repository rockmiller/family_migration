drop table if exists hik1850;
create table hik1850 as (
select distinct hik from cr1850 where hik is not null order by hik);

drop table if exists hik1860;
create table hik1860 as (
select distinct hik from cr1860 where hik is not null order by hik);