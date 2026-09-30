# Documentation

## data_processing_elections

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Ce traitement permet d'agréger les données des élections dans deux fichiers qui seront mis à jour à chaque nouvelle publication du Ministère de l'Intérieur. |
| Fréquence de mise à jour | Manuelle |
| Données sources | Fichiers standardisés à partir des données du Ministère de l'Intérieur, stockés sur notre S3 (`elections/sources/`) et listés dans `sources.json` avec leur jeu d'origine. Voir `scripts/` pour leur production. |
| Données de sorties | [Dataset données des élections agrégées](https://www.data.gouv.fr/datasets/donnees-des-elections-agregees/) |
| Channel Tchap d'information | bot-datagouv-dataeng |
