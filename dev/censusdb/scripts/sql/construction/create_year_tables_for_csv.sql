create table cs1850 as (select * from cper where cenyear = 1850 
order by cenyear, stateicp, countyicp, serial, relate, birthyr);

create table cs1860 as (select * from cper where cenyear = 1860 
order by cenyear, stateicp, countyicp, serial, relate, birthyr);

create table cs1870 as (select * from cper where cenyear = 1870
order by cenyear, stateicp, countyicp, serial, relate, birthyr);

create table cs1880 as (select * from cper where cenyear = 1880
order by cenyear, stateicp, countyicp, serial, relate, birthyr);

create table cs1900 as (select * from cper where cenyear = 1900 
order by cenyear, stateicp, countyicp, serial, relate, birthyr);

create table cs1910 as (select * from cper where cenyear = 1910
order by cenyear, stateicp, countyicp, serial, relate, birthyr);

create table cs1920 as (select * from cper where cenyear = 1920
order by cenyear, stateicp, countyicp, serial, relate, birthyr);
