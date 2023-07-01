ALTER TABLE dvf.stats_whole_period
ADD COLUMN libelle_geo VARCHAR(100);

ALTER TABLE dvf.stats_whole_period
ADD COLUMN echelle_geo VARCHAR(100);

UPDATE dvf.stats_whole_period swp 
SET echelle_geo = sd.echelle_geo 
FROM stats_dvf sd 
WHERE swp.code_geo = sd.code_geo;

UPDATE dvf.stats_whole_period swp 
SET libelle_geo = sd.libelle_geo 
FROM stats_dvf sd 
WHERE swp.code_geo = sd.code_geo;