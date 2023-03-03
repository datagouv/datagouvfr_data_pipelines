# Data Pipelines data.gouv.fr

Ce dépôt l'ensemble des DAGs Airflow de l'équipe data.gouv.fr.

Ces dags permettent de faire tourner des pipelines de données de différents types (ces types se reflètent dans la structure du dépôt) : 
- **data processing** : ces pipelines récupèrent des données existantes sur data.gouv.fr (ou ailleurs) et les traitent pour qu'ils soient plus facilement utilisables (ex: géocodage Sirene qui géocode l'ensemble de la base SIRENE)
- **dgv** : ces pipelines sont à usage interne de l'équipe data.gouv.fr. Ils permettent de monitorer l'activité de la plateforme
- **schema** : ces pipelines sont utilisés pour maintenir le site schema.data.gouv.fr et les traitements afférents.

## Ajout d'un DAG

- réaliser une PR sur ce dépôt en respectant la structure de celui-ci (créer un sous-dossier par traitement réalisé)
- variabiliser les paramètres de vos DAGs dans des variables Airflow