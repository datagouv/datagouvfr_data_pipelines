# Data Pipelines data.gouv.fr

Ce dépôt contient l'ensemble des DAGs Airflow de l'équipe data.gouv.fr. Le code source permettant de générer la stack airflow que nous utilisons est hébergé [sur ce dépôt](https://github.com/etalab/data-engineering-stack).

Il a pour objectif d'harmoniser les pratiques de traitements de données dans l'équipe data.gouv.fr et de répertorier au sein d'un même dépôt le maximum de ces traitements.

Ces dags permettent de faire tourner des pipelines de données de différents types (ces types se reflètent dans la structure du dépôt) : 
- **data processing** : ces pipelines récupèrent des données existantes sur data.gouv.fr (ou ailleurs) et les traitent pour qu'ils soient plus facilement utilisables (ex: géocodage Sirene qui géocode l'ensemble de la base SIRENE)
- **dgv** : ces pipelines sont à usage interne de l'équipe data.gouv.fr. Ils permettent de monitorer l'activité de la plateforme
- **schema** : ces pipelines sont utilisés pour maintenir le site schema.data.gouv.fr et les traitements afférents.

## Ajout d'un DAG

- réaliser une PR sur ce dépôt en respectant la structure de celui-ci (créer un sous-dossier par traitement réalisé)
- variabiliser les paramètres de vos DAGs dans des variables Airflow

## Linting
Ce dépôt est formaté avec [`ruff`](https://docs.astral.sh/ruff/) en [configuration par défaut](https://docs.astral.sh/ruff/configuration/), avant de commit :
```
ruff check --fix .
ruff format .
```
