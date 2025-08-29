SELECT c.name AS column_name, CAST(ep.value AS NVARCHAR(4000)) AS comment
FROM sys.columns c
         JOIN sys.tables t ON c.object_id = t.object_id
         LEFT JOIN sys.extended_properties ep
                   ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
WHERE t.name = :tname
  AND (SCHEMA_NAME(t.schema_id) = :sname OR :sname IS NULL)