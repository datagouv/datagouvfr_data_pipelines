# Documentation

## data_processing_sirene_geocodage

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG-sirene-geocodage-etalab.py`     |
| Description | DAG Airflow qui récupère les données de la base SIRENE publiées sur data.gouv.fr et lance un géocodage basé sur la BAL. Ce DAG dure plusieurs heures et dépose les fichiers sur files.data.gouv.fr/geo-sirene|
| Fréquence de mise à jour |  Mensuel (les 1er, 5 et 10 du mois) |
| Données sources | [JDD INSEE data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/) |
| Données de sorties | [JDD Etalab data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-etablissements-siret-geolocalisee-avec-la-base-dadresse-nationale-ban/) |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |

## Fichiers d'exécutions

L'ensemble des fichiers situés dans le dossier `remote_files` sont des copies des fichiers stockés sur le serveur distant et appelés par SSH dans le DAG.
Ils sont présents dans le dépôt à titre informatif et sont maintenus à jours manuellement.
