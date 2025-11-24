CREATE OR REPLACE VIEW features.occurrence AS

SELECT *,
ROUND(1- (num_identification_disagreements/ num_identification_agreements),2) as identification_agreement_rate

FROM preprocessed.gbif_citizen_labeled

