# Documentation

## data_processing_sirene_publication

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG-sirene-publication-insee.py`     |
| Description | DAG Airflow permettant de récupérer les données stock de la base SIRENE mises à disposition sur un site INSEE sécurisé. Ce DAG récupère les données et les [publie sur data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/). Ce JDD fait partie du Service Public de la Donnée (SPD) | Quotidien |
| Données sources | site INSEE sécurisé |
| Données de sorties | [JDD INSEE data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/) |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |


## data_processing_sirene_geolocalisation

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG-sirene-geolocalisation-insee.py`     |
| Description | DAG Airflow permettant de récupérer les données SIRENE géolocalisée par l'INSEE mises à disposition sur un site INSEE sécurisé. Ce DAG récupère les données et les [publie sur data.gouv.fr](https://www.data.gouv.fr/fr/datasets/geolocalisation-des-etablissements-du-repertoire-sirene-pour-les-etudes-statistiques/) | Quotidien |
| Données sources | site INSEE sécurisé |
| Données de sorties | [JDD INSEE data.gouv.fr](https://www.data.gouv.fr/fr/datasets/geolocalisation-des-etablissements-du-repertoire-sirene-pour-les-etudes-statistiques/) |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |
