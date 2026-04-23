DO $$ 
BEGIN
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dpe' AND TABLE_SCHEMA = 'dvf') THEN
        TRUNCATE TABLE dvf.dpe;
        DROP INDEX IF EXISTS parcelle_id_idx;
    ELSE
        CREATE UNLOGGED TABLE dvf.dpe (
            batiment_groupe_id VARCHAR(30),
            -- identifiant_dpe VARCHAR(13),
            -- type_batiment_dpe VARCHAR(11),
            periode_construction_dpe VARCHAR(10),
            -- annee_construction_dpe VARCHAR(5),
            -- date_etablissement_dpe VARCHAR(10),
            -- nombre_niveau_logement INT,
            nombre_niveau_immeuble INT,
            surface_habitable_immeuble DECIMAL(10,2),
            -- surface_habitable_logement DECIMAL(10,2),
            classe_bilan_dpe VARCHAR(1),
            classe_emission_ges VARCHAR(1),
            parcelle_id VARCHAR(14)
        );
    END IF;
END $$;