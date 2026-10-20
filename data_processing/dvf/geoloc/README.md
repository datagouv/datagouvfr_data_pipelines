# Documentation

## data_processing_dvf_geoloc

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Ce traitement met à jour les réexpositions géolocalisées des données DVF (fichiers annuels).  |
| Fréquence de mise à jour | Semestrielle |
| Données sources | [Jeu de données DVF](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres/) |
| Données de sorties | [Jeu de données](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees/) |
| Channel Tchap d'information | bot-datagouv-dataeng |

> Note : ce code est une reprise de https://github.com/datagouv/dvf

## data_processing_dvf_geoloc_concat

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Construit et publie le fichier concaténé `dvf.csv.gz` à partir des fichiers annuels déjà publiés par `data_processing_dvf_geoloc`. |
| Fréquence de mise à jour | Semestrielle |
| Données sources | Les fichiers annuels `full-*.csv.gz` publiés sur le jeu de données de destination. |
| Données de sorties | Le fichier unique `dvf.csv.gz`. |

## Processus manuel

Ce DAG est très lourd en mémoire, et provoque des erreurs OOM si on le lance en totalité sur une machine avec 12GB de RAM. Pour octobre 2026, on a effectué les actions suivantes à la main :

- lancer le DAG `data_processing_dvf_geoloc` pour chaque année (2021, 2022, 2023, 2024, 2025, 2026) en utilisant le paramètre `year_to_run`. À noter que pour 2021 et 2026, le fichier partiel (2021-s2 et 2026-s1) est correctement utilisé. Les fichiers sont stockés sur le jeu de données sur la plateforme de démo : https://demo.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees-3.
- lancer le DAG `data_processing_dvf_geoloc_concat` pour reconstruire le fichier concaténé `dvf.csv.gz` à partir des fichiers annuels déjà publiés (sans refaire l'enrichissement, qui est la partie gourmande en mémoire).
- télécharger en local toutes les ressources depuis ce jeu de données de démo, puis les uploader sur le dataset de production : https://data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees
- mettre à jour la description du jeu de données https://data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees.

Le travail de production des fichiers annuels et celui de la concaténation sont donc portés
par deux DAG distincts :

- `data_processing_dvf_geoloc` : télécharge, enrichit et publie les fichiers annuels
  `full-*.csv.gz`. Paramètres : `year_to_run` (debug, une seule année) et `delivery`
  (millesime du cadastre à utiliser).
- `data_processing_dvf_geoloc_concat` : télécharge les fichiers annuels déjà publiés,
  les concatène et publie le fichier unique `dvf.csv.gz`. Paramètre : `delivery`.
