-- counts per year (fast if index exists on cenyear)
SELECT cenyear, count(*) AS rows
FROM family.cper
GROUP BY cenyear
ORDER BY cenyear;