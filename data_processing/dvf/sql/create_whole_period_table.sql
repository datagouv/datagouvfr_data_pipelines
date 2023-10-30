DO $$ 
BEGIN
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'stats_whole_period' AND TABLE_SCHEMA = 'dvf') THEN
        TRUNCATE TABLE dvf.stats_whole_period;
    ELSE
        CREATE UNLOGGED TABLE dvf.stats_whole_period (
            code_geo VARCHAR(20),
            libelle_geo VARCHAR(100),
            code_parent VARCHAR(10),
            echelle_geo VARCHAR(15),
            nb_ventes_whole_appartement INT,
            moy_prix_m2_whole_appartement INT,
            med_prix_m2_whole_appartement INT,
            nb_ventes_whole_maison INT,
            moy_prix_m2_whole_maison INT,
            med_prix_m2_whole_maison INT,
            nb_ventes_whole_apt_maison INT,
            moy_prix_m2_whole_apt_maison INT,
            med_prix_m2_whole_apt_maison INT,
            nb_ventes_whole_local INT,
            moy_prix_m2_whole_local INT,
            med_prix_m2_whole_local INT,
            PRIMARY KEY (code_geo, code_parent)
        );
    END IF;
END $$;