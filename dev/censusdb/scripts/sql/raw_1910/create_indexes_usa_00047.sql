create index ix_cr1910_yser on cr1910(year, serial);
create index ix_cr1910_hik on cr1910(hik, year, serial) WHERE hik is not null;