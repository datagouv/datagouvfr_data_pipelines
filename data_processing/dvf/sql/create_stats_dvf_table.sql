DROP TABLE IF EXISTS stats_dvf CASCADE;
CREATE UNLOGGED TABLE stats_dvf (
code_geo VARCHAR(20),
nb_ventes_maison INT,
moy_prix_m2_maison INT,
med_prix_m2_maison INT,
nb_ventes_appartement INT,
moy_prix_m2_appartement INT,
med_prix_m2_appartement INT,
nb_ventes_local INT,
moy_prix_m2_local INT,
med_prix_m2_local INT,
annee_mois VARCHAR(7),
libelle_geo VARCHAR(100),
code_parent VARCHAR(10),
echelle_geo VARCHAR(15),
PRIMARY KEY (echelle_geo, code_geo, annee_mois, code_parent));
