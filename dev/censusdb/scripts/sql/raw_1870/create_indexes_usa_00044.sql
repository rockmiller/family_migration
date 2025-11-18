create index ix_cr1870_serial on cr1870(year, serial);
create index ix_cr1870_hikser on cr1870(hik, year, serial) WHERE hik is not null;


