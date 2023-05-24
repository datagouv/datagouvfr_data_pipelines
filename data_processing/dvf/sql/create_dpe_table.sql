DROP TABLE IF EXISTS dpe CASCADE;
CREATE UNLOGGED TABLE dpe (
date_etablissement_dpe VARCHAR(10),
etiquette_dpe VARCHAR(6),
etiquette_ges VARCHAR(6),
annee_construction VARCHAR(4),
type_batiment VARCHAR(11),
adresse_ban VARCHAR(300),
);
