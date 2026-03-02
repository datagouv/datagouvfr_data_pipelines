# Documentation

Ce sous-dossier contient 2 traitements.

## dgv_moderation_utilisateurs

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag_moderation-utilisateur.py`     |
| Description | Ce traitement permet de surveiller l'activité de la plateforme data.gouv.fr en termes de création d'utilisateurs et alerte en cas d'activité anormale (création de plus de 25 utilisateur sur la dernière heure). |
| Fréquence de mise à jour | Toutes les heures |
| Données sources | API data.gouv.fr |
| Données de sorties | Alerte Mattermost + Mail |
| Channel Mattermost d'information | ~datagouv-moderation-nouveautes |


## dgv_administrateur

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag_administrateur.py`     |
| Description | Ce traitement permet d'informer sur la liste des super-admins de data.gouv.fr |
| Fréquence de mise à jour | Tous les trois mois |
| Données sources | data.gouv.fr |
| Données de sorties | Alerte Mattermost |
| Channel Mattermost d'information | ~datagouv-moderation-nouveautes |
