CREATE TABLE IF NOT EXISTS public.metrics_datasets
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS public.metrics_reuses
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    reuse_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS public.metrics_organizations
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);
CREATE TABLE IF NOT EXISTS public.metrics_resources
(
    id SERIAL PRIMARY KEY,
    date_metric DATE,
    resource_id CHARACTER VARYING,
    dataset_id CHARACTER VARYING,
    organization_id CHARACTER VARYING,
    nb_visit INTEGER
);