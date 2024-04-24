DROP INDEX IF EXISTS code_commune_idx;
CREATE INDEX code_commune_idx ON dvf.dvf USING btree (code_commune);
DROP INDEX IF EXISTS section_prefixe_idx;
CREATE INDEX section_prefixe_idx ON dvf.dvf USING btree (section_prefixe);
