DROP TABLE IF EXISTS stats_dvf CASCADE;
CREATE UNLOGGED TABLE stats_dvf (
code_geo VARCHAR(20),
echelle_geo VARCHAR(15),
nb_ventes_maison FLOAT,
moy_prix_m2_maison FLOAT,
med_prix_m2_maison FLOAT,
nb_ventes_appartement FLOAT,
moy_prix_m2_appartement FLOAT,
med_prix_m2_appartement FLOAT,
nb_ventes_local FLOAT,
moy_prix_m2_local FLOAT,
med_prix_m2_local FLOAT,
annee_mois VARCHAR(7),
libelle_geo VARCHAR(100),
code_parent VARCHAR(10),
PRIMARY KEY (echelle_geo, code_geo, annee_mois, code_parent));
