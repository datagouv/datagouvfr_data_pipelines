DO $$ 
BEGIN
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'copro' AND TABLE_SCHEMA = 'dvf') THEN
        TRUNCATE TABLE dvf.copro;
    ELSE
        CREATE UNLOGGED TABLE dvf.copro (
            epci VARCHAR(9),
            commune VARCHAR(50),
            numero_immatriculation VARCHAR(10),
            type_syndic VARCHAR(13),
            identification_representant_legal VARCHAR(200),
            siret_representant_legal VARCHAR(50),
            code_ape VARCHAR(10),
            commune_representant_legal VARCHAR(50),
            mandat_en_cours_copropriete VARCHAR(50),
            nom_usage_copropriete VARCHAR(50),
            adresse_reference VARCHAR(200),
            numero_et_voie_adresse_reference VARCHAR(200),
            code_postal_adresse_reference VARCHAR(5),
            commune_adresse_reference VARCHAR(100),
            adresse_complementaire_1 VARCHAR(200),
            adresse_complementaire_2 VARCHAR(200),
            adresse_complementaire_3 VARCHAR(200),
            nombre_adresses_complementaires INT,
            long DECIMAL(12,10),
            lat DECIMAL(12,10),
            date_reglement_copropriete VARCHAR(11),
            residence_service VARCHAR(20),
            syndicat_cooperatif VARCHAR(9),
            syndicat_principal_ou_secondaire VARCHAR(3),
            si_secondaire_numero_immatriculation_principal VARCHAR(10),
            nombre_asl_rattache_syndicat_coproprietaires INT,
            nombre_aful_rattache_syndicat_coproprietaires INT,
            nombre_unions_syndicats_rattache_syndicat_coproprietaires INT,
            nombre_total_lots INT,
            nombre_total_lots_usage_habitation_bureaux_ou_commerces INT,
            nombre_lots_usage_habitation INT,
            nombre_lots_stationnement INT,
            nombre_arretes_code_sante_publique_en_cours INT,
            nombre_arretes_peril_parties_communes_en_cours INT,
            nombre_arretes_equipements_communs_en_cours INT,
            periode_construction VARCHAR(20),
            reference_cadastrale_1 VARCHAR(14),
            reference_cadastrale_2 VARCHAR(14),
            reference_cadastrale_3 VARCHAR(14),
            nombre_parcelles_cadastrales INT,
            nom_qp VARCHAR(100),
            code_qp VARCHAR(10),
            copro_dans_acv VARCHAR(3),
            copro_dans_pvd VARCHAR(3),
            PRIMARY KEY (numero_immatriculation)
        );
    END IF;
END $$;