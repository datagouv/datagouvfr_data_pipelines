DROP INDEX IF EXISTS code_commune_idx;
CREATE INDEX code_commune_idx ON dvf.dvf USING btree (code_commune);
DROP INDEX IF EXISTS section_prefixe_idx;
CREATE INDEX section_prefixe_idx ON dvf.dvf USING btree (section_prefixe);
DROP INDEX IF EXISTS parcelle_idx;
CREATE INDEX parcelle_idx ON dvf.dvf USING btree (id_parcelle, date_mutation DESC, id_mutation);
DROP INDEX IF EXISTS code_departement_idx;
CREATE INDEX code_departement_idx ON dvf.dvf USING btree (code_departement, date_mutation DESC, id_mutation);
