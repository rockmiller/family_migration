SELECT
    schemaname AS schema,
    tablename AS table,
    pg_size_pretty(pg_table_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) AS table_size,
    pg_size_pretty(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) AS total_size
FROM
    pg_tables
WHERE
    schemaname = 'family'
ORDER BY
    pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) DESC;