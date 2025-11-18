create index ix_cr1860_hik on cr1860(hik);
create index ix_cr1860_ser on cr1860(year, serial);
drop index if exists ix_cr1860_hikser;

create index ix_cr1870_hik on cr1870(hik);
create index ix_cr1870_ser on cr1870(year, serial);
drop index if exists ix_cr1870_hikser;

create index ix_cr1880_hik on cr1880(hik);
create index ix_cr1880_ser on cr1880(year, serial);
drop index if exists ix_cr1880_hikser;