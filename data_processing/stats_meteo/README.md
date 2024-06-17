# Documentation

## data_processing_meteo

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | Ce traitement permet de synchroniser les données du FTP source vers le Minio, et de créer et mettre à jour les JdD impactés sur data.gouv.fr. |
| Fréquence de mise à jour | Quotidienne |
| Données sources | Données issues du FTP mis à jour par Météo France |
| Données de sorties | Jeux de données [Météo France](https://www.data.gouv.fr/fr/organizations/meteo-france/#/datasets) |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |
