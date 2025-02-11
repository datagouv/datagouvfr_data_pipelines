DELETE
FROM metric.visits_datasets
WHERE date_metric = '%%date%%';

DELETE
FROM metric.visits_organizations
WHERE date_metric = '%%date%%';

DELETE
FROM metric.visits_resources
WHERE date_metric = '%%date%%';

DELETE
FROM metric.visits_reuses
WHERE date_metric = '%%date%%';

DELETE
FROM metric.visits_dataservices
WHERE date_metric = '%%date%%';
