# Documentation

Ce sous-dossier contient 4 traitements.

## dgv_digests

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG-digest.py`     |
| Description | Ce traitement permet de faire une synthèse de l'activité de data.gouv.fr sur une période quotidienne, hebdo (le lundi) et mensuelle (le 1er du mois).  |
| Fréquence de mise à jour | Quotidien |
| Données sources | API data.gouv.fr |
| Données de sorties | Stockés sur S3 |
| Channel Mattermost d'information | ~datagouv-activites |


## dgv_moderation_utilisateurs

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG-moderation-utilisateur.py`     |
| Description | Ce traitement permet de surveiller l'activité de la plateforme data.gouv.fr en termes de création d'utilisateurs et alerte en cas d'activité anormale (création de plus de 25 utilisateur sur la dernière heure). |
| Fréquence de mise à jour | Toutes les heures |
| Données sources | API data.gouv.fr |
| Données de sorties | Alerte Mattermost + Mail |
| Channel Mattermost d'information | ~datagouv-activites |


## dgv_tops

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG-tops.py`     |
| Description | Ce traitement permet de récupérer les jeux de données et réutilisations les plus visités lors de la journée, de la semaine et du mois précédent. |
| Fréquence de mise à jour | Quotidien |
| Données sources | Matomo / API stats.data.gouv.fr |
| Données de sorties | Alerte Mattermost |
| Channel Mattermost d'information | ~datagouv-reporting |
