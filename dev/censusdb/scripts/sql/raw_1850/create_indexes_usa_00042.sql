create index ix_cr1850_histid on cr1850(year, histid);
create index ix_cr1850_hik on cr1850(hik);
create index ix_cr1850_serial on cr1850(year, serial);
create index ix_cr1850_hikser on cr1850(hik, year, serial);