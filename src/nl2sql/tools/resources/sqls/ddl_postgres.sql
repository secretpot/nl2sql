SELECT a.attname, d.description
FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_attribute a ON a.attrelid = c.oid
         LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
WHERE c.relname = :table_name
  AND n.nspname = :schema
  AND a.attnum > 0
  AND NOT a.attisdropped