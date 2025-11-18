CREATE TABLE cper (
    cenyear SMALLINT NOT NULL,
	serial INTEGER NOT NULL,
	histid VARCHAR NOT NULL,
	hik VARCHAR,
	hhtype SMALLINT,
	histid_head VARCHAR,
	relate SMALLINT,
	birthyr SMALLINT,
	sex SMALLINT,
	race SMALLINT,
	bpl SMALLINT,
	histid_sp VARCHAR,
	histid_pop VARCHAR,
	histid_mom VARCHAR,
	steppop SMALLINT,
	stepmom SMALLINT,
	stateicp VARCHAR,
	countyicp VARCHAR,
	farm SMALLINT,
	urban SMALLINT,
	sizepl SMALLINT,
	CONSTRAINT cpkey UNIQUE (cenyear, serial, histid));

	
	