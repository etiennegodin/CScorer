
-- create table with name matches
CREATE OR REPLACE TABLE clean.inat_observers AS

SELECT *
FROM raw.inat_observers_extracted
WHERE id_string = "name"
OR id_string = "login";

-- removes duplicates 
DELETE FROM clean.inat_observers
WHERE id_string IN(
                    SELECT id_string 
                    FROM clean.inat_observers 
                    GROUP BY id_string 
                    HAVING COUNT(*) > 1
);

DELETE FROM clean.inat_observers
WHERE universal_search_rank < 1;

CREATE OR REPLACE TABLE raw.discarded_inat_observers AS

WITH all_removed AS(
    SELECT DISTINCT id_string
    FROM raw.inat_observers_extracted
    WHERE id_string NOT IN(
                        SELECT id_string FROM clean.inat_observers
                        )
)

SELECT * 
FROM all_removed;



