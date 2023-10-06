# Documentation

## data_processing_inpi_dirigeants

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | DAG Airflow permettant de récupérer les données INPI IMR (Immatriculations). Le DAG se connecte au FTP de l'INPI et sauvegarde les données sur une instance Minio. Ce traitement est notamment utilisé pour générer le fichier stock dirigeants pour [l'annuaire des entreprises](https://annuaire-entreprises.data.gouv.fr) | Quotidien |
| Données sources | FTP INPI |
| Données de sorties | Base sqlite dans Minio |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |
