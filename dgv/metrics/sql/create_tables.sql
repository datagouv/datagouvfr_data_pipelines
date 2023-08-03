-- Manually run the following:
-- CREATE SCHEMA IF NOT EXISTS airflow;
-- CREATE DATABASE airflow;

-- Logs visits tables
CREATE TABLE IF NOT EXISTS airflow.visits_datasets
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.visits_reuses
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    reuse_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.visits_organizations
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.visits_resources
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    resource_id CHARACTER VARYING,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);

-- Matomo tables
CREATE TABLE IF NOT EXISTS airflow.matomo_datasets
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.matomo_reuses
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    reuse_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);
CREATE TABLE IF NOT EXISTS airflow.matomo_organizations
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);


-- Aggregated metrics tables
CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.metrics_datasets AS
    SELECT visits.__id as __id,
            COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.dataset_id, matomo.dataset_id) as dataset_id,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           visits.nb_visit,
           matomo.nb_outlink,
           resources.nb_visit as resource_nb_visit
    FROM airflow.visits_datasets visits
    FULL OUTER JOIN airflow.matomo_datasets matomo
    ON visits.dataset_id = matomo.dataset_id AND
       visits.date_metric = matomo.date_metric
    LEFT OUTER JOIN (
        SELECT dataset_id, date_metric, sum(nb_visit) as nb_visit FROM airflow.visits_resources
        GROUP BY dataset_id, date_metric
    ) resources
    ON COALESCE(visits.dataset_id, matomo.dataset_id) = resources.dataset_id AND
       COALESCE(visits.date_metric, matomo.date_metric) = resources.date_metric
;


CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.metrics_reuses AS
    SELECT visits.__id as __id,
           COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.reuse_id, matomo.reuse_id) as reuse_id,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           visits.nb_visit,
           matomo.nb_outlink
    FROM airflow.visits_reuses visits
    FULL OUTER JOIN airflow.matomo_reuses matomo
    ON visits.reuse_id = matomo.reuse_id AND
       visits.date_metric = matomo.date_metric
;
CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.metrics_organizations AS
    SELECT visits.__id as __id,
           COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           datasets.nb_visit as dataset_nb_visit,
           datasets.resource_nb_visit as resource_nb_visit,
           reuses.nb_visit as reuse_nb_visit,
           matomo.nb_outlink
    FROM airflow.visits_organizations visits
    FULL OUTER JOIN airflow.matomo_organizations matomo
    ON visits.organization_id = matomo.organization_id AND
       visits.date_metric = matomo.date_metric
    LEFT OUTER JOIN (
        SELECT organization_id, date_metric, sum(nb_visit) as nb_visit, sum(resource_nb_visit) as resource_nb_visit
        FROM airflow.metrics_datasets
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
CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.datasets AS
    SELECT
        MIN(__id) as __id,
        dataset_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit,
        sum(nb_outlink) as monthly_outlink,
        sum(resource_nb_visit) as monthly_visit_resource
    FROM airflow.metrics_datasets
    GROUP BY metric_month, dataset_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.reuses AS
    SELECT
        MIN(__id) as __id,
        reuse_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit,
        sum(nb_outlink) as monthly_outlink
    FROM airflow.metrics_reuses
    GROUP BY metric_month, reuse_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.organizations AS
    SELECT
        MIN(__id) as __id,
        organization_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(dataset_nb_visit) as monthly_visit_dataset,
        sum(resource_nb_visit) as monthly_visit_resource,
        sum(reuse_nb_visit) as monthly_visit_reuse,
        sum(nb_outlink) as monthly_outlink
    FROM airflow.metrics_organizations
    GROUP BY metric_month, organization_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.resources AS
    SELECT
        MIN(__id) as __id,
        resource_id,
        dataset_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit_resource
    FROM airflow.visits_resources
    GROUP BY metric_month, resource_id, dataset_id
;

-- Global site table
CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.site AS
    SELECT __id,
           COALESCE(datasets.metric_month, reuses.metric_month) as metric_month,
           datasets.monthly_visit as monthly_visit_dataset,
           datasets.monthly_visit_resource as monthly_visit_resource,
           reuses.monthly_visit as monthly_visit_reuse,
           reuses.monthly_outlink as monthly_outlink
    FROM (
        SELECT MIN(__id) as __id,
               metric_month,
               sum(monthly_visit) as monthly_visit,
               sum(monthly_visit_resource) as monthly_visit_resource
        FROM airflow.datasets GROUP BY metric_month ) datasets
    FULL OUTER JOIN (
        SELECT metric_month,
        sum(monthly_visit) as monthly_visit,
        sum(monthly_outlink) as monthly_outlink
        FROM airflow.reuses GROUP BY metric_month ) reuses
    ON datasets.metric_month = reuses.metric_month
;

-- Sum tables
CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.datasets_total AS
    SELECT
        MIN(__id) as __id,
        dataset_id,
        sum(nb_visit) as visit,
        sum(nb_outlink) as outlink,
        sum(resource_nb_visit) as visit_resource
    FROM airflow.metrics_datasets
    GROUP BY dataset_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.reuses_total AS
    SELECT
        MIN(__id) as __id,
        reuse_id,
        sum(nb_visit) as visit,
        sum(nb_outlink) as outlink
    FROM airflow.metrics_reuses
    GROUP BY reuse_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.organizations_total AS
    SELECT
        MIN(__id) as __id,
        organization_id,
        sum(dataset_nb_visit) as visit_dataset,
        sum(resource_nb_visit) as visit_resource,
        sum(reuse_nb_visit) as visit_reuse,
        sum(nb_outlink) as outlink
    FROM airflow.metrics_organizations
    GROUP BY organization_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS airflow.resources_total AS
    SELECT
        MIN(__id) as __id,
        resource_id,
        dataset_id,
        sum(nb_visit) as visit_resource
    FROM airflow.visits_resources
    GROUP BY resource_id, dataset_id
;


CREATE INDEX IF NOT EXISTS visits_datasets_dataset_id ON airflow.visits_datasets USING btree (dataset_id);
CREATE INDEX IF NOT EXISTS visits_datasets_date_metric ON airflow.visits_datasets USING btree (date_metric);
CREATE INDEX IF NOT EXISTS visits_datasets_organization_id ON airflow.visits_datasets USING btree (organization_id);

CREATE INDEX IF NOT EXISTS visits_organizations_organization_id ON airflow.visits_organizations USING btree (organization_id);
CREATE INDEX IF NOT EXISTS visits_organizations_date_metric ON airflow.visits_organizations USING btree (date_metric);

CREATE INDEX IF NOT EXISTS visits_reuses_reuse_id ON airflow.visits_reuses USING btree (reuse_id);
CREATE INDEX IF NOT EXISTS visits_reuses_date_metric ON airflow.visits_reuses USING btree (date_metric);
CREATE INDEX IF NOT EXISTS visits_reuses_organization_id ON airflow.visits_reuses USING btree (organization_id);

CREATE INDEX IF NOT EXISTS visits_resources_resource_id ON airflow.visits_resources USING btree (resource_id);
CREATE INDEX IF NOT EXISTS visits_resources_date_metric ON airflow.visits_resources USING btree (date_metric);
CREATE INDEX IF NOT EXISTS visits_resources_dataset_id ON airflow.visits_resources USING btree (dataset_id);
