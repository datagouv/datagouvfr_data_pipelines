DROP TABLE IF EXISTS dpe CASCADE;
CREATE UNLOGGED TABLE dpe (
date_etablissement_dpe VARCHAR(10),
etiquette_dpe VARCHAR(20),
etiquette_ges VARCHAR(20),
annee_construction VARCHAR(4),
type_batiment VARCHAR(11),
adresse_ban CHARACTER VARYING
);
