create table csearch as (
select distinct hik, cenyear, serial, stateicp || countyicp as loc
from cper
where hik is not null);