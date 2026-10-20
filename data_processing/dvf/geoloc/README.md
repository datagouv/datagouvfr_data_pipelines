# Documentation

## data_processing_dvf_geoloc

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Ce traitement met à jour les réexpositions géolocalisées des données DVF.  |
| Fréquence de mise à jour | Semestrielle |
| Données sources | [Jeu de données DVF](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres/) |
| Données de sorties | [Jeu de données](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees/) |
| Channel Tchap d'information | bot-datagouv-dataeng |

> Note : ce code est une reprise de https://github.com/datagouv/dvf

## Processus manuel

Ce DAG est très lourd en mémoire, et provoque des erreurs OOM si on le lance en totalité sur une machine avec 12GB de RAM. Pour octobre 2026, on a effectué les actions suivantes à la main :

- lancer le DAG pour chaque année (2021, 2022, 2023, 2024, 2025, 2026) en utilisant le paramètre `year_to_run`. À noter que pour 2021 et 2026, le fichier partiel (2021-s2 et 2026-s1) est correctement utilisé. Les fichiers sont stockés sur le jeu de données sur la plateforme de démo : https://demo.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees-3.
- lancer le DAG une nouvelle fois avec le paramètre `only="only-concatenated"` pour reconstruire le fichier concaténé `dvf.csv.gz` à partir des fichiers annuels déjà publiés (sans refaire l'enrichissement, qui est la partie gourmande en mémoire).
- télécharger en local toutes les ressources depuis ce jeu de données de démo, puis les uploader sur le dataset de production : https://data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees
- mettre à jour la description du jeu de données https://data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees.

## Paramètre `only`

Le DAG accepte un paramètre de lancement `only` pour restreindre une exécution
manuelle à une partie seulement du pipeline (sans dupliquer le DAG) :

| Valeur | Comportement |
| -------- | -------- |
| `none` (défaut) | Exécute tout : enrichissement annuel **et** fichier concaténé `dvf.csv.gz`. |
| `only-concatenated` | Construit/republic **uniquement le fichier concaténé**, en réutilisant les fichiers `full-*.csv.gz` déjà publiés sur le jeu de données de destination (les tâches lourdes d'enrichissement sont sautées). |
| `only-yearly` | Enrichit et publie **uniquement les fichiers annuels** `full-*.csv.gz` (le fichier concaténé est sauté). |

Le paramètre `year_to_run` reste prioritaire sur `only` (mode debug : on ne traite
qu'une seule année, le concaténé n'a pas de sens tant que la fenêtre est incomplète).
