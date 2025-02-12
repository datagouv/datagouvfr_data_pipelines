DELETE
FROM metric.matomo_dataservices
WHERE date_metric = '%%date%%';

DELETE
FROM metric.matomo_datasets
WHERE date_metric = '%%date%%';

DELETE
FROM metric.matomo_reuses
WHERE date_metric = '%%date%%';

DELETE
FROM metric.matomo_organizations
WHERE date_metric = '%%date%%';
