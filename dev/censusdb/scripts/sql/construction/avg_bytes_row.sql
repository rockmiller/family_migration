-- sample N rows per year and get per-year avg size
WITH sample AS (
  SELECT *
  FROM family.cper
  WHERE cenyear IN (1880, 1900, 1920)  -- pick representative years
  ORDER BY random()
  LIMIT 300000
)
SELECT cenyear, avg(pg_column_size(t)) AS avg_row_bytes
FROM (SELECT * FROM sample) t
GROUP BY cenyear
ORDER BY cenyear;