CREATE TABLE cs1900
AS SELECT (
YEAR::integer as cenyear
RIGHT('00000000' || SERIAL,8) CHAR(8) as cenhh
HHTYPE CHAR(1) AS hhtype
GQ CHAR(1) as gq
FARM CHAR(1) as farm

RIGHT('00' || STATEICP,2) CHAR(2) AS stateicp
RIGHT('0000'|| COUNTYICP,4) CHAR(4) AS countyicp
RIGHT('00' || STATEICP,2) || RIGHT('0000'|| COUNTYICP,4) CHAR(6) as loc
RIGHT('00' || SIZEPL,2) CHAR(2) as sizepl
RIGHT('00' || URBAN,1) CHAR(2) as sizepl
URBAN CHAR(1) as urban

PERNUM::INTEGER AS pernum
FAMUNIT::INTEGER as famunit
RIGHT('00' || RELATE,2) CHAR(2) AS relate
RIGHT('0000'|| RELATED,4) CHAR(4) AS related
SEX CHAR(1) as sex
AGE::INTEGER as age
BIRTHYR:INTEGER as birthyr
RACE CHAR(1) as race
RIGHT('000' || RACED,3) CHAR(3) AS raced
RIGHT('000' || BPL,3) CHAR(3) AS bpl
RIGHT('0000' || BPLD,4) CHAR(4) AS bpld
NATIVITY CHAR(1) AS nativity
CITIZEN CHAR(1) as citizen
SCHOOL CHAR(1) as school
LABFORCE CHAR(1) as labforce
RIGHT('000' || OCC1950,3) CHAR(3) as occ1950
RIGHT(REPEAT('0', 36) || HISTID,36) CHAR(36) as histid
RIGHT(REPEAT('0', 36) || HISTID_HEAD,36) CHAR(36) as histid_head
RIGHT(REPEAT('0', 36) || HISTID_SP,36) CHAR(36) as histid_sp
RIGHT(REPEAT('0', 36) || HISTID_POP,36) CHAR(36) as histid_pop
RIGHT(REPEAT('0', 36) || HISTID_MOM,36) CHAR(36) as histid_mom
RIGHT(REPEAT('0', 21) || HIK,21) CHAR(21) as hik
RIGHT(REPEAT('0', 21) || HIK_HEAD,21) CHAR(21) as hik_head
RIGHT(REPEAT('0', 21) || HIK_SP,21) as CHAR(21) as hik_sp
RIGHT(REPEAT('0', 21) || HIK_POP,21) as hik_pop
RIGHT(REPEAT('0', 21) || HIK_MOM,21) as hik_mom
SURSIM::INTEGER as sursim
STEPMOM CHAR(1) as stepmom
STEPPOP CHAR(1) as steppop
);

(union to census)

