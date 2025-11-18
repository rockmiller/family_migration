-- replace 100000 with a sample size you like; this samples rows and computes avg pg_column_size

SELECT avg(pg_column_size(t)) AS avg_row_bytes
FROM (
  SELECT * FROM family.cper WHERE cenyear = 1900 LIMIT 100000
) t;