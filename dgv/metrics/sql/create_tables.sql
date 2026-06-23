-- Logs visits tables
CREATE TABLE IF NOT EXISTS metric.visits_datasets
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER DEFAULT 0,
    nb_visit_apis INTEGER DEFAULT 0,
    nb_visit_total INTEGER DEFAULT 0,
    nb_visit_api1 INTEGER DEFAULT 0,
    nb_visit_api2 INTEGER DEFAULT 0,
    nb_visit_fr INTEGER DEFAULT 0,
    nb_visit_en INTEGER DEFAULT 0,
    nb_visit_es INTEGER DEFAULT 0,
    nb_visit_ INTEGER DEFAULT 0,
    nb_visit_static INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metric.visits_reuses
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    reuse_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER DEFAULT 0,
    nb_visit_apis INTEGER DEFAULT 0,
    nb_visit_total INTEGER DEFAULT 0,
    nb_visit_api1 INTEGER DEFAULT 0,
    nb_visit_api2 INTEGER DEFAULT 0,
    nb_visit_fr INTEGER DEFAULT 0,
    nb_visit_en INTEGER DEFAULT 0,
    nb_visit_es INTEGER DEFAULT 0,
    nb_visit_ INTEGER DEFAULT 0,
    nb_visit_static INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metric.visits_organizations
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER DEFAULT 0,
    nb_visit_apis INTEGER DEFAULT 0,
    nb_visit_total INTEGER DEFAULT 0,
    nb_visit_api1 INTEGER DEFAULT 0,
    nb_visit_api2 INTEGER DEFAULT 0,
    nb_visit_fr INTEGER DEFAULT 0,
    nb_visit_en INTEGER DEFAULT 0,
    nb_visit_es INTEGER DEFAULT 0,
    nb_visit_ INTEGER DEFAULT 0,
    nb_visit_static INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metric.visits_dataservices
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataservice_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER DEFAULT 0,
    nb_visit_apis INTEGER DEFAULT 0,
    nb_visit_total INTEGER DEFAULT 0,
    nb_visit_api1 INTEGER DEFAULT 0,
    nb_visit_api2 INTEGER DEFAULT 0,
    nb_visit_fr INTEGER DEFAULT 0,
    nb_visit_en INTEGER DEFAULT 0,
    nb_visit_es INTEGER DEFAULT 0,
    nb_visit_ INTEGER DEFAULT 0,
    nb_visit_static INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metric.visits_resources
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    resource_id CHARACTER VARYING,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER DEFAULT 0,
    nb_visit_apis INTEGER DEFAULT 0,
    nb_visit_total INTEGER DEFAULT 0,
    nb_visit_api1 INTEGER DEFAULT 0,
    nb_visit_api2 INTEGER DEFAULT 0,
    nb_visit_fr INTEGER DEFAULT 0,
    nb_visit_en INTEGER DEFAULT 0,
    nb_visit_es INTEGER DEFAULT 0,
    nb_visit_ INTEGER DEFAULT 0,
    nb_visit_static INTEGER DEFAULT 0,
    nb_visit_api_permalink INTEGER DEFAULT 0
);

-- Matomo tables
CREATE TABLE IF NOT EXISTS metric.matomo_datasets
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);
CREATE TABLE IF NOT EXISTS metric.matomo_reuses
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    reuse_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);
CREATE TABLE IF NOT EXISTS metric.matomo_organizations
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);
CREATE TABLE IF NOT EXISTS metric.matomo_dataservices
(
    __id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataservice_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_outlink INTEGER
);


-- Aggregated metrics tables
CREATE MATERIALIZED VIEW IF NOT EXISTS metric.metrics_datasets AS
    SELECT visits.__id as __id,
           COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.dataset_id, matomo.dataset_id) as dataset_id,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           visits.nb_visit,
           matomo.nb_outlink,
           resources.nb_visit as resource_nb_download
    FROM metric.visits_datasets visits
    FULL OUTER JOIN metric.matomo_datasets matomo
    ON visits.dataset_id = matomo.dataset_id AND
       visits.date_metric = matomo.date_metric
    LEFT OUTER JOIN (
        SELECT dataset_id, date_metric, sum(nb_visit) as nb_visit FROM metric.visits_resources
        GROUP BY dataset_id, date_metric
    ) resources
    ON COALESCE(visits.dataset_id, matomo.dataset_id) = resources.dataset_id AND
       COALESCE(visits.date_metric, matomo.date_metric) = resources.date_metric
;


CREATE MATERIALIZED VIEW IF NOT EXISTS metric.metrics_reuses AS
    SELECT visits.__id as __id,
           COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.reuse_id, matomo.reuse_id) as reuse_id,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           visits.nb_visit,
           matomo.nb_outlink
    FROM metric.visits_reuses visits
    FULL OUTER JOIN metric.matomo_reuses matomo
    ON visits.reuse_id = matomo.reuse_id AND
       visits.date_metric = matomo.date_metric
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.metrics_dataservices AS
    SELECT visits.__id as __id,
           COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.dataservice_id, matomo.dataservice_id) as dataservice,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           visits.nb_visit,
           matomo.nb_outlink
    FROM metric.visits_dataservices AS visits
    FULL OUTER JOIN metric.matomo_dataservices AS matomo
    ON visits.dataservice_id = matomo.dataservice_id AND
       visits.date_metric = matomo.date_metric
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.metrics_organizations AS
    SELECT visits.__id as __id,
           COALESCE(visits.date_metric, matomo.date_metric) as date_metric,
           COALESCE(visits.organization_id, matomo.organization_id) as organization_id,
           datasets.nb_visit as dataset_nb_visit,
           datasets.resource_nb_download as resource_nb_download,
           reuses.nb_visit as reuse_nb_visit,
           dataservices.nb_visit as dataservice_nb_visit,
           matomo.nb_outlink
    FROM metric.visits_organizations visits
    FULL OUTER JOIN metric.matomo_organizations matomo
    ON visits.organization_id = matomo.organization_id AND
       visits.date_metric = matomo.date_metric
    LEFT OUTER JOIN (
        SELECT organization_id, date_metric, sum(nb_visit) as nb_visit, sum(resource_nb_download) as resource_nb_download
        FROM metric.metrics_datasets
        GROUP BY organization_id, date_metric
    ) datasets
    ON COALESCE(visits.organization_id, matomo.organization_id) = datasets.organization_id AND
       COALESCE(visits.date_metric, matomo.date_metric) = datasets.date_metric
    LEFT OUTER JOIN (
        SELECT organization_id, date_metric, sum(nb_visit) as nb_visit FROM metric.metrics_reuses
        GROUP BY organization_id, date_metric
    ) reuses
    ON COALESCE(visits.organization_id, matomo.organization_id) = reuses.organization_id AND
       COALESCE(visits.date_metric, matomo.date_metric) = reuses.date_metric
    LEFT OUTER JOIN (
        SELECT organization_id, date_metric, sum(nb_visit) as nb_visit FROM metric.metrics_dataservices
        GROUP BY organization_id, date_metric
    ) dataservices
    ON COALESCE(visits.organization_id, matomo.organization_id) = dataservices.organization_id AND
       COALESCE(visits.date_metric, matomo.date_metric) = dataservices.date_metric
;

-- Monthly aggregated metrics tables
CREATE MATERIALIZED VIEW IF NOT EXISTS metric.datasets AS
    SELECT
        MIN(__id) as __id,
        dataset_id,
        organization_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit,
        sum(resource_nb_download) as monthly_download_resource
    FROM metric.metrics_datasets
    GROUP BY metric_month, dataset_id, organization_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.reuses AS
    SELECT
        MIN(__id) as __id,
        reuse_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit
    FROM metric.metrics_reuses
    GROUP BY metric_month, reuse_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.organizations AS
    SELECT
        MIN(__id) as __id,
        organization_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(dataset_nb_visit) as monthly_visit_dataset,
        sum(resource_nb_download) as monthly_download_resource,
        sum(reuse_nb_visit) as monthly_visit_reuse,
        sum(dataservice_nb_visit) as monthly_visit_dataservice
    FROM metric.metrics_organizations
    GROUP BY metric_month, organization_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.resources AS
    SELECT
        MIN(__id) as __id,
        resource_id,
        dataset_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_download_resource
    FROM metric.visits_resources
    GROUP BY metric_month, resource_id, dataset_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.dataservices AS
    SELECT
        MIN(__id) as __id,
        dataservice_id,
        to_char(date_trunc('month', date_metric) , 'YYYY-mm') AS metric_month,
        sum(nb_visit) as monthly_visit
    FROM metric.visits_dataservices
    GROUP BY metric_month, dataservice_id
;

-- Global site table
CREATE MATERIALIZED VIEW IF NOT EXISTS metric.site AS
    SELECT __id,
           COALESCE(datasets.metric_month, reuses.metric_month) as metric_month,
           datasets.monthly_visit as monthly_visit_dataset,
           datasets.monthly_download_resource as monthly_download_resource,
           reuses.monthly_visit as monthly_visit_reuse
    FROM (
        SELECT MIN(__id) as __id,
               metric_month,
               sum(monthly_visit) as monthly_visit,
               sum(monthly_download_resource) as monthly_download_resource
        FROM metric.datasets GROUP BY metric_month ) datasets
    FULL OUTER JOIN (
        SELECT metric_month,
        sum(monthly_visit) as monthly_visit
        FROM metric.reuses GROUP BY metric_month ) reuses
    ON datasets.metric_month = reuses.metric_month
    FULL OUTER JOIN (
        SELECT metric_month,
        sum(monthly_visit) as monthly_visit
        FROM metric.dataservices GROUP BY metric_month ) dataservices
    ON datasets.metric_month = dataservices.metric_month
;

-- Sum tables
CREATE MATERIALIZED VIEW IF NOT EXISTS metric.datasets_total AS
    SELECT
        MIN(__id) as __id,
        dataset_id,
        sum(nb_visit) as visit,
        sum(nb_outlink) as outlink,
        sum(resource_nb_download) as download_resource
    FROM metric.metrics_datasets
    GROUP BY dataset_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.reuses_total AS
    SELECT
        MIN(__id) as __id,
        reuse_id,
        sum(nb_visit) as visit,
        sum(nb_outlink) as outlink
    FROM metric.metrics_reuses
    GROUP BY reuse_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.organizations_total AS
    SELECT
        MIN(__id) as __id,
        organization_id,
        sum(dataset_nb_visit) as visit_dataset,
        sum(resource_nb_download) as download_resource,
        sum(reuse_nb_visit) as visit_reuse,
        sum(dataservice_nb_visit) as visit_dataservice,
        sum(nb_outlink) as outlink
    FROM metric.metrics_organizations
    GROUP BY organization_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.resources_total AS
    SELECT
        MIN(__id) as __id,
        resource_id,
        dataset_id,
        sum(nb_visit) as download_resource
    FROM metric.visits_resources
    GROUP BY resource_id, dataset_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric.dataservices_total AS
    SELECT
        MIN(__id) AS __id,
        dataservice_id,
        SUM(nb_visit) AS visit
    FROM metric.visits_dataservices
    GROUP BY dataservice_id
;

-- Index on visits table
CREATE INDEX IF NOT EXISTS visits_datasets_dataset_id ON metric.visits_datasets USING btree (dataset_id);
CREATE INDEX IF NOT EXISTS visits_datasets_date_metric ON metric.visits_datasets USING btree (date_metric);
CREATE INDEX IF NOT EXISTS visits_datasets_organization_id ON metric.visits_datasets USING btree (organization_id);

CREATE INDEX IF NOT EXISTS visits_organizations_organization_id ON metric.visits_organizations USING btree (organization_id);
CREATE INDEX IF NOT EXISTS visits_organizations_date_metric ON metric.visits_organizations USING btree (date_metric);

CREATE INDEX IF NOT EXISTS visits_reuses_reuse_id ON metric.visits_reuses USING btree (reuse_id);
CREATE INDEX IF NOT EXISTS visits_reuses_date_metric ON metric.visits_reuses USING btree (date_metric);
CREATE INDEX IF NOT EXISTS visits_reuses_organization_id ON metric.visits_reuses USING btree (organization_id);

CREATE INDEX IF NOT EXISTS visits_resources_resource_id ON metric.visits_resources USING btree (resource_id);
CREATE INDEX IF NOT EXISTS visits_resources_date_metric ON metric.visits_resources USING btree (date_metric);
CREATE INDEX IF NOT EXISTS visits_resources_dataset_id ON metric.visits_resources USING btree (dataset_id);

CREATE INDEX IF NOT EXISTS visits_dataservices_dataservice_id ON metric.visits_dataservices USING btree (dataservice_id);
CREATE INDEX IF NOT EXISTS visits_dataservices_organization_id ON metric.visits_dataservices USING btree (organization_id);
CREATE INDEX IF NOT EXISTS visits_dataservices_date_metric ON metric.visits_dataservices USING btree (date_metric);

-- UNIQUE index on each materialized view: required by REFRESH MATERIALIZED
-- VIEW CONCURRENTLY (see refresh_materialized_views in task.py), which avoids
-- blocking reads from the metrics API during the refresh.
-- The key follows each view's grain: the GROUP BY for aggregated views
-- (uniqueness guaranteed by construction), the (id, date_metric) couple for the
-- base metrics_* views (sources deduplicated by date by the
-- visit/matomo_postgres_duplication_safety tasks).
-- These unique indexes also act as a plain index for lookups on their prefix:
-- no need for a separate single-column index on the 1st column.
CREATE UNIQUE INDEX IF NOT EXISTS metrics_datasets_unique ON metric.metrics_datasets USING btree (dataset_id, date_metric);
CREATE UNIQUE INDEX IF NOT EXISTS metrics_reuses_unique ON metric.metrics_reuses USING btree (reuse_id, date_metric);
CREATE UNIQUE INDEX IF NOT EXISTS metrics_dataservices_unique ON metric.metrics_dataservices USING btree (dataservice, date_metric);
CREATE UNIQUE INDEX IF NOT EXISTS metrics_organizations_unique ON metric.metrics_organizations USING btree (organization_id, date_metric);

CREATE UNIQUE INDEX IF NOT EXISTS datasets_unique ON metric.datasets USING btree (dataset_id, organization_id, metric_month);
CREATE UNIQUE INDEX IF NOT EXISTS reuses_unique ON metric.reuses USING btree (reuse_id, metric_month);
CREATE UNIQUE INDEX IF NOT EXISTS organizations_unique ON metric.organizations USING btree (organization_id, metric_month);
CREATE UNIQUE INDEX IF NOT EXISTS resources_unique ON metric.resources USING btree (resource_id, dataset_id, metric_month);
CREATE UNIQUE INDEX IF NOT EXISTS dataservices_unique ON metric.dataservices USING btree (dataservice_id, metric_month);

CREATE UNIQUE INDEX IF NOT EXISTS site_unique ON metric.site USING btree (metric_month);

CREATE UNIQUE INDEX IF NOT EXISTS datasets_total_unique ON metric.datasets_total USING btree (dataset_id);
CREATE UNIQUE INDEX IF NOT EXISTS reuses_total_unique ON metric.reuses_total USING btree (reuse_id);
CREATE UNIQUE INDEX IF NOT EXISTS organizations_total_unique ON metric.organizations_total USING btree (organization_id);
CREATE UNIQUE INDEX IF NOT EXISTS resources_total_unique ON metric.resources_total USING btree (resource_id, dataset_id);
CREATE UNIQUE INDEX IF NOT EXISTS dataservices_total_unique ON metric.dataservices_total USING btree (dataservice_id);

-- Secondary indexes not covered by the unique index prefix (API filters on
-- metric_month alone, or on organization_id alone for datasets).
CREATE INDEX IF NOT EXISTS datasets_metric_month ON metric.datasets USING btree (metric_month);
CREATE INDEX IF NOT EXISTS datasets_organization_id ON metric.datasets USING btree (organization_id);
CREATE INDEX IF NOT EXISTS organizations_metric_month ON metric.organizations USING btree (metric_month);
CREATE INDEX IF NOT EXISTS reuses_metric_month ON metric.reuses USING btree (metric_month);
CREATE INDEX IF NOT EXISTS resources_metric_month ON metric.resources USING btree (metric_month);
CREATE INDEX IF NOT EXISTS dataservices_metric_month ON metric.dataservices USING btree (metric_month);

-- One-shot cleanup of the old single-column indexes on the key id: redundant
-- now that they are the prefix of the unique index above. Idempotent DROP
-- (no-op on later runs), placed after the unique indexes are created.
DROP INDEX IF EXISTS metric.datasets_dataset_id;
DROP INDEX IF EXISTS metric.organizations_organization_id;
DROP INDEX IF EXISTS metric.reuses_reuse_id;
DROP INDEX IF EXISTS metric.resources_resource_id;
DROP INDEX IF EXISTS metric.dataservices_dataservice_id;
DROP INDEX IF EXISTS metric.datasets_total_dataset_id;
DROP INDEX IF EXISTS metric.organizations_total_organization_id;
DROP INDEX IF EXISTS metric.reuses_total_reuse_id;
DROP INDEX IF EXISTS metric.resources_total_resource_id;
DROP INDEX IF EXISTS metric.dataservices_total_dataservice_id;
