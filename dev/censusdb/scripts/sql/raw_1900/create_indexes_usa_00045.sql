create index ix_cr1880_serial on cr1880(year, serial);
create index ix_cr1880_hikser on cr1880(hik, year, serial) WHERE hik is not null;