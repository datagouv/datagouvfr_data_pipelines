DROP TABLE IF EXISTS distribution_prix CASCADE;
CREATE UNLOGGED TABLE distribution_prix (
code_geo VARCHAR(20),
type_local VARCHAR(11),
xaxis TEXT,
yaxis TEXT,
PRIMARY KEY (code_geo, type_local));
