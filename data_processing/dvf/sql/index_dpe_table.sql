DROP INDEX IF EXISTS parcelle_id_idx;
CREATE INDEX parcelle_id_idx ON dvf.dpe USING btree (parcelle_id);
