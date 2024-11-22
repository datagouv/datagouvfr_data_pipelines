# Documentation

## dgv_notification_activite

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | Ce traitement permet de surveiller l'activité de la plateforme data.gouv.fr et informe au fil de l'eau des nouveaux jeux de données / réutilisations / organisations créés sur la plateforme. Lorsqu'un nouveau dataset est détecté, le traitement essaie d'analyser si celui-ci obéit à un schéma spécifique. |
| Fréquence de mise à jour | Toutes les heures |
| Données sources | API data.gouv.fr |
| Données de sorties | Alerte Mattermost |
| Channel Mattermost d'information | ~datagouv-activites / si schéma : ~datagouv-schema-activites |
