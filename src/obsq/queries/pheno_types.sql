SELECT 
DISTINCT attribute_label, value_label


FROM clean.inat_phenology
GROUP BY attribute_label, value_label
ORDER BY attribute_label