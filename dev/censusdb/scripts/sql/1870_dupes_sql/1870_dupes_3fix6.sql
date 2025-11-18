alter table cr1870_dupes_rows
	drop column if exists row, 
	drop column if exists pernum, 
	drop column if exists famunit,
	drop column if exists nfams,
	drop column if exists ncouples,
	drop column if exists nfathers,
	drop column if exists nmothers,
	drop column if exists nchild,
	drop column if exists nchlt5,
	drop column if exists nsibs,
	drop column if exists eldch,
	drop column if exists yngch,
	drop column if exists momrule_hist,
	drop column if exists poprule_hist;

alter table cr1870_f2
	drop column if exists pernum, 
	drop column if exists famunit,
	drop column if exists nfams,
	drop column if exists ncouples,
	drop column if exists nfathers,
	drop column if exists nmothers,
	drop column if exists nchild,
	drop column if exists nchlt5,
	drop column if exists nsibs,
	drop column if exists eldch,
	drop column if exists yngch,
	drop column if exists momrule_hist,
	drop column if exists poprule_hist;

drop index if exists ix_cr1870f2_histid;

drop table if exists cr1870_fixes;
create table cr1870_fixes as (
select distinct b.*
from cr1870_3fix3 a
left join cr1870_dupes_rows b on a.composite_key = b.composite_key
where b.composite_key is not null);




