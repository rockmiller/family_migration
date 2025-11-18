drop table if exists bstmp1;
create table bstmp1 as (

select src as hik from bslk
union
select tgt as hik from bslk);

create index bstmp1_hik on bstmp1(hik);
	
drop table if exists bstp;
create table bstp as (

-- attaching attributes for study population

select distinct on (h.hik) h.hik, p.sex, race, birthyr, bpl, farm, 
	case when s.slvhcat is null then '0' else s.slvhcat end as slvhcat,
	case when d.sex is not null then '1' else '0' end as adult
from pcpp as p
inner join bstmp1 as h on p.hik = h.hik
left join slv as s on s.hik = h.hik
left join padl as d on d.hik = h.hik
order by hik, birthyr, farm, sex, race);
	
create index bstp_hik on bstp(hik);
drop table if exists bstmp1;