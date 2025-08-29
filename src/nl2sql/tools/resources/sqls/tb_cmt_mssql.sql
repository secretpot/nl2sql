SELECT CAST(ep.value AS NVARCHAR(4000)) AS comment
FROM sys.extended_properties ep
         JOIN sys.tables t ON ep.major_id = t.object_id
WHERE ep.minor_id = 0
  AND ep.name = 'MS_Description'
  AND t.name = :tname
  AND (SCHEMA_NAME(t.schema_id) = :sname OR :sname IS NULL)