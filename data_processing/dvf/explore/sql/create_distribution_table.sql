DO $$ 
BEGIN
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'distribution_prix' AND TABLE_SCHEMA = 'dvf') THEN
        TRUNCATE TABLE dvf.distribution_prix;
    ELSE
        CREATE UNLOGGED TABLE dvf.distribution_prix (
            code_geo VARCHAR(20),
            type_local VARCHAR(11),
            xaxis TEXT,
            yaxis TEXT,
            PRIMARY KEY (code_geo, type_local)
        );
    END IF;
END $$;