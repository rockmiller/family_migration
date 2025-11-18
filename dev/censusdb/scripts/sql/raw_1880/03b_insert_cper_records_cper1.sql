INSERT INTO cper (
	cenyear, serial, histid, hik, hhtype, histid_head, relate, 
	birthyr, sex, race, bpl, histid_sp, histid_pop, histid_mom, steppop, stepmom,
	stateicp, countyicp, farm, urban, sizepl)
SELECT 
	CAST(NULLIF(TRIM(year), '') AS SMALLINT) AS cenyear,
	CAST(NULLIF(TRIM(serial), '') AS INTEGER) AS serial,
	histid,
	NULLIF(TRIM(hik), '') AS hik,
	CAST(NULLIF(TRIM(hhtype), '') AS SMALLINT) AS hhtype,
	histid_head,
	CAST(NULLIF(TRIM(relate), '') AS SMALLINT) AS relate,
	CAST(NULLIF(TRIM(birthyr), '') AS SMALLINT) AS birthyr,
	CAST(NULLIF(TRIM(sex), '') AS SMALLINT) AS hhtype,
	CAST(NULLIF(TRIM(race), '') AS SMALLINT) AS race,
	CAST(NULLIF(TRIM(bpl), '') AS SMALLINT) AS bpl,
	histid_sp,
	histid_pop,
	histid_mom,
	CAST(NULLIF(TRIM(steppop), '') AS SMALLINT) AS steppop,
	CAST(NULLIF(TRIM(stepmom), '') AS SMALLINT) AS stepmom,
	RIGHT('00' || stateicp,2) as stateicp,
	RIGHT('0000' || countyicp,4) as countyicp,
	CAST(NULLIF(TRIM(farm), '') AS SMALLINT) AS farm,
	CAST(NULLIF(TRIM(urban), '') AS SMALLINT) AS urban,
	CAST(NULLIF(TRIM(sizepl), '') AS SMALLINT) AS sizepl
FROM cper1
ON CONFLICT ON CONSTRAINT cpkey DO NOTHING;