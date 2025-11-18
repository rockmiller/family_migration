-- adds a composite key to cr1870

drop table if exists cr1870_f1;
create table cr1870_f1 as (
select b.*, 
	COALESCE(b.histid, repeat('$', length(b.histid))) ||
	COALESCE(b.histid_head, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_sp, repeat('$',length(b.histid))) ||
  	COALESCE(b.histid_pop, repeat('$', length(b.histid))) ||
  	COALESCE(b.histid_mom, repeat('$', length(b.histid))) AS composite_key
from cr1870 b);

create index idx_cr1870f1_composite on cr1870_f1(composite_key);

	