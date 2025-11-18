-- step 2: get households corresponding to base links and summarize hiks in those households
-- union households from cr_yyyy
-- union to basehiks for comprehensive list of links in study population
-- this is a recursive process (tested with just one round)

drop table if exists studyhiks;
create table studyhiks as (

select distinct a.hik, year, serial
from basehiks a
left join cr1870 b on a.hik = b.hik
where b.hik is not null);


-- all the census households that contain the base hiks


-- create index ix_studyhiks_hik on studyhiks(hik);
-- create index ix_studyhiks on studyhiks(year, serial);

-- drop table if exists basehiks
