DELETE FROM cr1870
WHERE ctid IN (
    WITH numbered_rows AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY year, serial, histid
                ORDER BY relate ASC -- Keep the row with the smallest 'relate' value
            ) as rnum
        FROM
            cr1870
    )
    SELECT ctid
    FROM numbered_rows
    WHERE rnum > 1
);
