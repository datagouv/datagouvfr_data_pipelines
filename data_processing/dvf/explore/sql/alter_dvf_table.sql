ALTER TABLE dvf.dvf
ADD section_prefixe VARCHAR(5);

UPDATE dvf.dvf
SET section_prefixe = SUBSTRING(id_parcelle, 6, 5);