drop index if exists idx_cr1870_f2_ck;
drop index if exists ix_1870f2_ck;

drop table if exists cr1870_f3;
create table cr1870_f3 as (
select * from cr1870_f2
union
select * from cr1870_fixes);

create unique index ix_1870f3_h on cr1870_f3(histid);



