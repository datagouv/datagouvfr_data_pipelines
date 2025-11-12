-- Logs calls table
CREATE TABLE IF NOT EXISTS metric.calls_tabular
(
    resource_id CHARACTER VARYING,
    date_metric DATE,
    nb_calls INTEGER DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS tabular_resource_date_idx ON metric.calls_tabular (resource_id, date_metric);

CREATE TABLE IF NOT EXISTS metric.tabular_processed
(
    file_name CHARACTER VARYING,
    date_processed DATE
);
