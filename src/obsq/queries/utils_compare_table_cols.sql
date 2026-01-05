-- Columns in table1 but not in table2
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'table1'
EXCEPT
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'table2';

-- Columns in table2 but not in table1
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'table2'
EXCEPT
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'table1';
