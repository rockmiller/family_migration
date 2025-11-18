-- Revised pklk only (assumes pcpp - union of zones - is already complete)

drop table if exists pklk;
create table pklk as (

with

-- get the set of unique hiks in the batch

uhik as (
select distinct on(hik) hik, sex from pcpp),

-- get the unique links between parent and child records

ad as (
select distinct c.famcode, c.cenyear, c.rolecode, c.hik, c.sex
from pcpp as c
inner join uhik as b on c.hik = b.hik
where c.hik is not null and c.rolecode in ('1','2')
order by famcode),

ch as (
select distinct c.famcode, c.cenyear, c.rolecode, b.hik as phik, c.hik as chik, 
	b.sex as bsx, c.sex as csx
from pcpp as c
inner join ad as b on b.famcode = c.famcode
where c.rolecode in ('3')
and c.hik is not null),

-- getting descent links

dlk1 as (
select distinct on (phik, chik) phik, chik, bsx, csx
from ch
order by phik, chik, bsx, csx),

dlk as (
select chik as src, phik as tgt, bsx as edg, csx as sxs, bsx as sxt,
1.0::float as weight
from dlk1
order by src, edg, tgt),

-- marriage links

fam as (
select distinct c.famcode, c.cenyear, c.rolecode, c.hik, c.sex
from pcpp as c
inner join uhik as b on c.hik = b.hik
where c.hik is not null and rolecode in ('1','2')
order by famcode),

cpm as (
select distinct b.famcode, b.rolecode, m.hik, m.sex
from fam as b
inner join uhik as m on m.hik = b.hik
where b.rolecode = '1'),

cpf as (
select distinct b.famcode, b.rolecode, f.hik, f.sex
from fam as b
inner join uhik as f on f.hik = b.hik
where b.rolecode = '2'),
	
cp as (
select b.famcode, m.hik as src, f.hik as tgt, '0'::char(1) as edg, m.sex as sxs, f.sex as sxt
from fam as b
left join cpm as m on m.famcode = b.famcode
left join cpf as f on f.famcode = b.famcode),

mlk as (
select distinct on (src, tgt) src, tgt, edg, sxs, sxt, 1.0::float as weight
from cp
where src is not null and tgt is not null and src != tgt),
	
un as (
select * from dlk union select * from mlk)
	
select * from un);

create index pklk_src on pklk(src);
create index pklk_tgt on pklk(tgt);
create index pklk_edg on pklk(edg);