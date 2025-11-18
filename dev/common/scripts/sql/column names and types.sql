SELECT 
    column_name, 
    data_type,
    COALESCE(character_maximum_length, numeric_precision) AS size_precision
FROM 
    information_schema.columns
WHERE 
    table_schema = 'family' -- Replace 'public' with your schema name if different
    AND table_name = 'cper' -- Replace 'your_table_name' with your table's name
ORDER BY 
    ordinal_position; -- Ensures columns are in the order they appear in the table

