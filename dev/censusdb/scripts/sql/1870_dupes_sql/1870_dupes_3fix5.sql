-- index the dupes table
create index ix_cr1870_dupes_ck on cr1870_dupes(composite_key);

-- extract non-duplicated records from cr1870_f1 (cr1870_f2)
drop table if exists cr1870_f2;
create table cr1870_f2 as (
select distinct a.*
from cr1870_f1 a
left join cr1870_dupes b on a.composite_key = b.composite_key
where b.composite_key is null);

-- index the non-duplicated records
create index idx_cr1870_f2_ck on cr1870_f2(composite_key);






