drop table if exists searchlinks;
create table searchlinks as (

select hik, year, serial
from cr1870 
where hik is not null
group by hik, year, serial

union

select hik, year, serial
from cr1880
where hik is not null
group by hik, year, serial

order by hik, year
);

drop index if exists ix_search_hik;
drop index if exists ix_search_ys;

create index ix_search_hik on searchlinks(hik);
create index ix_search_ys on searchlinks(year, serial);



