drop table if exists bslk;
create table bslk as (

with

-- first set of ancestor links to base hiks (mhik matching src)
	
anc1 as (
select src, tgt, edg, weight, sxs, sxt
from pklk
inner join bpop on bpop.hik = pklk.src
where pklk.edg in ('1','2')
order by src, edg, tgt),

-- ancestors of ancestor links from anc1 (tgt matching src)

anc2 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct tgt as hik from anc1) as a
on a.hik = pklk.src
where pklk.edg in ('1','2')
order by hik, edg),

-- ancestors of anc2 links (tgt matching src)

anc3 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct tgt as hik from anc2) as a
on a.hik = pklk.src
where pklk.edg in ('1','2')
order by hik, edg),

-- consolidate bhikkb and anc hiks for descendent search

anklc as (
select * from anc1	
union select * from anc2
union select * from anc3),

mhik2 as (
select distinct src as hik from anklc
union select distinct tgt as hik from anklc
union select hik from bhikkb),

-- find descent links (tgt matching mhik2)

des1 as (
select src, tgt, edg, weight, sxs, sxt
from pklk
inner join mhik2 on mhik2.hik = pklk.tgt
where pklk.edg in ('1','2')
order by src, edg, tgt),

-- descendants of descendants from des1 (src matching tgt)

des2 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct src as hik from des1) as a
on a.hik = pklk.tgt
where pklk.edg in ('1','2')
order by hik, edg),

-- descendants of descendants from des2 (src matching tgt)
	
des3 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct src as hik from des2) as a
on a.hik = pklk.tgt
where pklk.edg in ('1','2')
order by hik, edg),

-- consolidate descendant hiks with mhik2

deslc as (
select * from des1
union select * from des2
union select * from des3),

mhik3 as (
select distinct src as hik from deslc
union select distinct tgt as hik from deslc
union select hik from mhik2),

-- find anc links round 2 (src matching tgt)

anc4 as (
select src, tgt, edg, weight, sxs, sxt
from pklk
inner join mhik3 on mhik3.hik = pklk.src
where pklk.edg in ('1','2')
order by src, edg, tgt),

anc5 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct tgt as hik from anc4) as a
on a.hik = pklk.src
where pklk.edg in ('1','2')
order by hik, edg),

anc6 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct tgt as hik from anc5) as a
on a.hik = pklk.src
where pklk.edg in ('1','2')
order by hik, edg),

anklc2 as (
select * from anc4	
union select * from anc5
union select * from anc6),

mhik4 as (
select distinct src as hik from anklc2
union select distinct tgt as hik from anklc2
union select hik from mhik3),
	
-- repeat descent links
-- find descent links (tgt matching mhik4)

des4 as (
select src, tgt, edg, weight, sxs, sxt
from pklk
inner join mhik4 on mhik4.hik = pklk.tgt
where pklk.edg in ('1','2')
order by src, edg, tgt),

-- descendants of descendants from des4 (src matching tgt)

des5 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct src as hik from des4) as a
on a.hik = pklk.tgt
where pklk.edg in ('1','2')
order by hik, edg),

-- descendants of descendants from des6 (src matching tgt)
	
des6 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct src as hik from des5) as a
on a.hik = pklk.tgt
where pklk.edg in ('1','2')
order by hik, edg),

-- consolidate descendant hiks with mhik4

deslc2 as (
select * from des4
union select * from des5
union select * from des6),

mhik5 as (
select distinct src as hik from deslc2
union select distinct tgt as hik from deslc2
union select hik from mhik4),
	
-- find anc links round 3 (src matching tgt)

anc7 as (
select src, tgt, edg, weight, sxs, sxt
from pklk
inner join mhik5 on mhik5.hik = pklk.src
where pklk.edg in ('1','2')
order by src, edg, tgt),

anc8 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct tgt as hik from anc7) as a
on a.hik = pklk.src
where pklk.edg in ('1','2')
order by hik, edg),

anc9 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct tgt as hik from anc8) as a
on a.hik = pklk.src
where pklk.edg in ('1','2')
order by hik, edg),

anklc3 as (
select * from anc7
union select * from anc8
union select * from anc9),

mhik6 as (
select distinct src as hik from anklc3
union select distinct tgt as hik from anklc3
union select hik from mhik5),
	
-- repeat descent links
-- find descent links (tgt matching mhik4)

des7 as (
select src, tgt, edg, weight, sxs, sxt
from pklk
inner join mhik6 on mhik6.hik = pklk.tgt
where pklk.edg in ('1','2')
order by src, edg, tgt),

-- descendants of descendants from des4 (src matching tgt)

des8 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct src as hik from des7) as a
on a.hik = pklk.tgt
where pklk.edg in ('1','2')
order by hik, edg),

-- descendants of descendants from des6 (src matching tgt)
	
des9 as (
select src, tgt, edg, weight, sxs, sxt
from pklk inner join
(select distinct src as hik from des8) as a
on a.hik = pklk.tgt
where pklk.edg in ('1','2')
order by hik, edg),

-- consolidate descendant hiks with mhik6

deslc3 as (
select * from des7
union select * from des8
union select * from des9),

mhik7 as (
select distinct src as hik from deslc2
union select distinct tgt as hik from deslc2
union select hik from mhik6),

-- pulling all lineage links together
--dlk2 as (	
dlk1 as (
select * from anklc
union select * from deslc
union select * from anklc2
union select * from deslc2
union select * from anklc3
union select * from deslc3
order by src, edg, tgt),

-- trimming off the links that are only children
--(REMOVED)

--dlk1 as (
--select src,tgt,edg,weight,sxs,sxt from dlk2
--inner join padl on padl.hik = dlk2.src),

-- get marriage links corresponding to descent links
	
mh as (
select src as hik from dlk1 where sxs in ('1')
union
select tgt as hik from dlk1 where sxt in ('1')),

fh as (
select src as hik from dlk1 where sxs in ('2')
union
select tgt as hik from dlk1 where sxt in ('2')),

mar as (
select a.src, a.tgt, a.edg, a.weight, a.sxs, a.sxt
from pklk as a
inner join mh on mh.hik = a.src
where edg = '0'
union
select a.src, a.tgt, a.edg, a.weight, a.sxs, a.sxt
from pklk as a
inner join fh on fh.hik = a.tgt
where edg = '0')
	
-- consolidate the marriage links and the descent links
select * from mar
union select * from dlk1
);

create index bslk_src on bslk(src);
create index bslk_tgt on bslk(tgt);
create index bslk_edg on bslk(edg);