    SET description = 
        CASE
            WHEN i.description IS NOT NULL THEN 1
            ELSE 0
        END