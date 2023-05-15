-- Manually run the following:
-- CREATE SCHEMA IF NOT EXISTS airflow;
-- CREATE DATABASE airflow;

-- Logs visits tables
CREATE TABLE IF NOT EXISTS airflow.visits_datasets
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.visits_reuses
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    reuse_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.visits_organizations
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.visits_resources
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    resource_id CHARACTER VARYING,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);

-- Matomo tables
CREATE TABLE IF NOT EXISTS airflow.matomo_datasets
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.matomo_reuses
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    reuse_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.matomo_organizations
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);

-- Aggregated metrics tables
CREATE OR REPLACE VIEW airflow.metrics_datasets AS
    SELECT COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.dataset_id, matomo.dataset_id) as dataset_id,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           visits.nb_visit,
           matomo.nb_outlink
    FROM airflow.visits_datasets visits
    FULL OUTER JOIN airflow.matomo_datasets matomo
    ON visits.dataset_id = matomo.dataset_id AND
       visits.date_metric = matomo.date_metric
;
CREATE OR REPLACE VIEW airflow.metrics_reuses AS
    SELECT COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.reuse_id, matomo.reuse_id) as reuse_id,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           visits.nb_visit,
           matomo.nb_outlink
    FROM airflow.visits_reuses visits
    FULL OUTER JOIN airflow.matomo_reuses matomo
    ON visits.reuse_id = matomo.reuse_id AND
       visits.date_metric = matomo.date_metric
;
CREATE OR REPLACE VIEW airflow.metrics_organizations AS
    SELECT COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           datasets.nb_visit as dataset_nb_visit,
           reuses.nb_visit as reuse_nb_visit,
           matomo.nb_outlink
    FROM airflow.visits_organizations visits
    FULL OUTER JOIN airflow.matomo_organizations matomo
    ON visits.organization_id = matomo.organization_id AND
       visits.date_metric = matomo.date_metric
    LEFT OUTER JOIN (
        SELECT organization_id, date_metric, sum(nb_visit) as nb_visit FROM airflow.metrics_datasets
        GROUP BY organization_id, date_metric
    ) datasets
    ON COALESCE(visits.organization_id, matomo.organization_id) = datasets.organization_id AND
       COALESCE(visits.date_metric, matomo.date_metric) = datasets.date_metric
    LEFT OUTER JOIN (
        SELECT organization_id, date_metric, sum(nb_visit) as nb_visit FROM airflow.metrics_reuses
        GROUP BY organization_id, date_metric
    ) reuses
    ON COALESCE(visits.organization_id, matomo.organization_id) = reuses.organization_id AND
       COALESCE(visits.date_metric, matomo.date_metric) = reuses.date_metric
;

-- Monthly aggregated metrics tables
CREATE OR REPLACE VIEW airflow.datasets AS
    SELECT
        dataset_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit,
        sum(nb_outlink) as monthly_outlink
    FROM airflow.metrics_datasets
    GROUP BY metric_month, dataset_id
;

CREATE OR REPLACE VIEW airflow.reuses AS
    SELECT
        reuse_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit,
        sum(nb_outlink) as monthly_outlink
    FROM airflow.metrics_reuses
    GROUP BY metric_month, reuse_id
;

CREATE OR REPLACE VIEW airflow.organizations AS
    SELECT
        organization_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(dataset_nb_visit) as monthly_visit_dataset,
        sum(reuse_nb_visit) as monthly_visit_reuse,
        sum(nb_outlink) as monthly_outlink
    FROM airflow.metrics_organizations
    GROUP BY metric_month, organization_id
;
