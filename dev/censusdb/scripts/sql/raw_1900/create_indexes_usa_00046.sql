create index ix_cr1900_yser on cr1900(year, serial);
create index ix_cr1900_hik on cr1900(hik, year, serial) WHERE hik is not null;