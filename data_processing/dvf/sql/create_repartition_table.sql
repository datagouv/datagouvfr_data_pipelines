DROP TABLE IF EXISTS repartition_prix CASCADE;
CREATE UNLOGGED TABLE repartition_prix (
code_geo VARCHAR(20),
repartition TEXT,
PRIMARY KEY (code_geo));
