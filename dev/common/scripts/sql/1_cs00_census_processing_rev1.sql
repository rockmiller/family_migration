drop table if exists cptw;
create table cptw as (
	
-- famcodes
WITH cpx as (	
SELECT
CAST(CASE 
WHEN (sploc <>'00') AND (sex = '1') THEN year || serial || right(pernum,2) || sploc
WHEN (sploc <>'00') AND (sex = '2') THEN year || serial || sploc || right(pernum,2)
WHEN (sploc = '00') AND ((momloc <> '00') OR (poploc <>'00')) THEN (year || serial || poploc || momloc)
WHEN (sploc = '00') AND (poploc = '00' and momloc = '00') AND (sex = '1') THEN year || serial || right(pernum,2) || sploc
WHEN (sploc = '00') AND (poploc = '00' and momloc = '00') AND (sex = '2') THEN year || serial || sploc || right(pernum,2)
ELSE NULL END as char(16)) as famcode,
year::smallint AS cenyear, 

--rolecodes

CAST(CASE 
WHEN (sploc <>'00') AND (sex = '1') THEN '1'
WHEN (sploc <>'00') AND (sex = '2') THEN '2'
WHEN (momloc = '00' and poploc = '00') and (sploc = '00') and (sex = '1') THEN '1'
WHEN (momloc = '00' and poploc = '00') and (sploc = '00') and (sex = '2') THEN '2'
WHEN ((momloc <> '00') OR (poploc <>'00')) AND (sploc = '00') THEN '3'
ELSE '0' END as char(1)) as rolecode,

CASE WHEN birthyr = '9999' THEN (year::smallint - age::smallint)::smallint
	 WHEN birthyr is null then (year::smallint - age::smallint)::smallint
	ELSE birthyr::smallint END AS birthyr,
	
sex AS sex,
race AS race,
age::smallint AS age,
CASE WHEN nativity LIKE '% %' THEN null
ELSE nativity END AS nativity,
bpl AS bpl,
labforce AS labforce,
sploc AS sploc,
poploc AS poploc,
momloc AS momloc,
steppop,
stepmom,
CASE WHEN relate = '01' THEN TRUE ELSE FALSE END as hhead,

CASE WHEN (sploc = '00') THEN TRUE
ELSE FALSE END as unmarried,

CASE WHEN ((momloc <> '00') OR (poploc <>'00')) THEN TRUE
ELSE FALSE END as child,
	
CASE WHEN ((momloc <> '00') OR (poploc <>'00')) AND (sploc = '00') THEN TRUE
ELSE FALSE END as unmc,

CASE WHEN (age::smallint < 18) AND (sploc = '00') AND (labforce <> '2') THEN TRUE 
ELSE FALSE END as minor,

histid AS histid,
gq as gq,
gqtype as gqtype
hhtype AS hhtype,
relate AS relate,
CASE WHEN multgen LIKE '% %' THEN null ELSE multgen END	AS multgen,
famunit,
farm,
stateicp,
countyicp,
stateicp || countyicp AS countycode,
enumdist,
CASE WHEN hik LIKE '%     %' THEN null
ELSE hik END	AS hik,
CASE WHEN hik NOT LIKE '%     %' THEN true
ELSE false END as hiklink,
	
-- raw census keys

CAST(year || serial as char(12)) AS household,
CAST(year || serial || pernum as char(16)) AS personcode,
	
-- slaveholder information
case 
	when slavenum not like '% %' then slavenum::smallint 
	else 0 end as slavenum,
case 
	when slaveown not like '% %' then slaveown::smallint 
	else 0 end as slaveown,
case
	when slavenum not like '% %'and slavenum::smallint >= 20 then '3'
	when slavenum not like '% %'and slavenum::smallint BETWEEN 6 AND 19 then '2'
	when slavenum not like '% %'and slavenum::smallint BETWEEN 1 AND 5 then '1'
	else '0' 
	end as slvhcat	
	
FROM cen15
-- WHERE (hhtype IN ('1','2','3') AND gq in ('1','2'))
ORDER BY famcode, rolecode, birthyr, sex DESC
),

families as (
select famcode, unmc, count(*) as fm 
from cpx
group by famcode, unmc
order by famcode, unmc),

adults as (
select families.famcode as fc, fm as ad from families
where unmc = false
order by families.famcode, unmc),

children as (
select families.famcode as fc, fm as ch from families
where unmc is true
order by families.famcode),

famcounts as (
select distinct adults.fc, ad, ch from adults
	left join children on adults.fc = children.fc),

cfuf as (
select * from famcounts
where (ad = 1 and ch IS NOT NULL) OR ad = 2),

final as (
select
fc as famcode,
dense_rank() over (partition by cenyear, countycode order by famcode) as famno,
cenyear,
rolecode as rolecode,
CAST(ROW_NUMBER() OVER (PARTITION BY fc ORDER BY rolecode, birthyr)
as smallint) as famseq,
birthyr,
sex,
race,
personcode as personcode,
age,
nativity,
bpl,
marst,
labforce,
slavenum,
slvhcat,
countycode,
enumdist,
hhead as hhead,
CASE
WHEN (rolecode = '1') AND (ad = 2) AND (unmc = FALSE) THEN TRUE
WHEN (rolecode in ('1','2')) AND (ad = 1) AND (unmc = FALSE) THEN TRUE
ELSE false END as famhead,
unmarried as unmarried,
child as child,
unmc as unmc,
minor as minor,
CASE WHEN (ch IS NOT NULL) and (unmc = FALSE) THEN TRUE
	ELSE FALSE
	END as parent,
CASE WHEN (ad = 2) and (unmc = FALSE) THEN TRUE
	ELSE FALSE
	END as couple,
CAST(ad as smallint) as famadults,
CAST(ch as smallint) as famchildren,
CASE WHEN steppop != '0' and stepmom !='0' THEN true else false end as stepfam,
household as household,
histid as histid,
gq as gq,
hhtype as hhtype,
multgen as multgen,
famunit,
farm,
relate as relate,
TRUE as cfu,
CASE WHEN hik LIKE '%    %' THEN NULL
ELSE hik END as hik,
hiklink,
(select batch from bcf order by batch desc limit 1) as batch
from cfuf
left join cpx on cfuf.fc = cpx.famcode
order by personcode),

reorder as (
select
personcode,
cenyear,
birthyr,
sex,
race,
age,
marst,
labforce,
nativity,
bpl,
famcode,
rolecode,
famhead,
famseq,
stepfam,
parent,
couple,
unmarried,
child,
minor,
hhtype,
famunit,
hhead,
relate,
multgen,
farm,
slavenum,
slvhcat,
countycode,
CASE WHEN enumdist LIKE '%  %' THEN null ELSE enumdist END as enumdist,
hiklink,
hik,
batch
from final
order by personcode)

select * from reorder
order by personcode
);

-- indexing the table

CREATE INDEX cptw_famcode on cptw(famcode);
CREATE INDEX cptw_personcode on cptw(personcode);
CREATE INDEX cptw_hik on cptw(hik);


