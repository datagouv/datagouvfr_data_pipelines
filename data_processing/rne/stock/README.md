# Documentation

## data_processing_rne_dirigeants

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | DAG Airflow permettant de récupérer les données stock du RNE (INPI). Le DAG récupère le fichier zip depuis le site de l'INPI, parcours les fichiers json, nettoie et sauvegarde les données sur une instance Minio. Ce traitement est utilisé pour générer le fichier stock dirigeants pour [l'annuaire des entreprises](https://annuaire-entreprises.data.gouv.fr). | Quotidien |
| Données sources | RNE INPI |
| Données de sorties | Fichiers CSV dans Minio |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |
