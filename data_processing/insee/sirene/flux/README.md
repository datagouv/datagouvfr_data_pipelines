# Documentation

## data_processing_sirene_flux

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG-flux-sirene.py`     |
| Description | DAG Airflow permettant de récupérer les données flux de la base SIRENE. Ces données sont récupérées via l'API SIRENE de l'INSEE. La récupération des données concernent les données non diffusibles et modification des entreprises durant le mois courant | Quotidien |
| Données sources | API Sirene INSEE |
| Données de sorties | Minio |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |