# Documentation

## data_processing_pnt_monitor

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Ce traitement permet de suivre les anomalies dans les données PNT de Météo France et de construire le fichier de l'arborescence du bucket |
| Fréquence de mise à jour | Quotidienne |
| Données sources | Jeux de données du [topic PNT](https://www.data.gouv.fr/datasets/?topic=65e0c82c2da27c1dff5fa66f) |
| Données de sorties | [Jeu de données](https://www.data.gouv.fr/datasets/66d02b7174375550d7b10f3f) sur l'état du bucket PNT |
| Channel Tchap d'information | bot-datagouv-dataeng |
