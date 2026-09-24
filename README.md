# Data Pipelines data.gouv.fr

Ce dépôt contient l'ensemble des DAGs Airflow de l'équipe data.gouv.fr. Le code source permettant de générer la stack airflow que nous utilisons est hébergé [sur ce dépôt](https://github.com/datagouv/data-engineering-stack/).

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

After cloning this repository, don't forget to install https://pre-commit.com/ and run `pre-commit install` to install the git hook scripts.

<details><summary>Example of pre-commit output</summary>
When committing for the first time after the install, you should see pre-commit running:

<pre><code>
(.venv) ➜  datagouvfr_data_pipelines git:(add-doc-for-pre-commit) ✗ gcmsg "docs: add sentence on installing pre-commit"
[INFO] Initializing environment for https://github.com/pre-commit/pre-commit-hooks.
[INFO] Initializing environment for https://github.com/astral-sh/ruff-pre-commit.
[INFO] Initializing environment for https://github.com/pre-commit/mirrors-mypy.
[INFO] Initializing environment for https://github.com/pre-commit/mirrors-mypy:tokenize-rt==3.2.0,types-requests,types-psutil,types-redis.
[INFO] Installing environment for https://github.com/pre-commit/pre-commit-hooks.
[INFO] Once installed this environment will be reused.
[INFO] This may take a few minutes...
[INFO] Installing environment for https://github.com/astral-sh/ruff-pre-commit.
[INFO] Once installed this environment will be reused.
[INFO] This may take a few minutes...
[INFO] Installing environment for https://github.com/pre-commit/mirrors-mypy.
[INFO] Once installed this environment will be reused.
[INFO] This may take a few minutes...
check yaml...........................................(no files to check)Skipped
check json...........................................(no files to check)Skipped
check toml...........................................(no files to check)Skipped
detect private key.......................................................Passed
fix end of files.........................................................Passed
trim trailing whitespace.................................................Passed
debug statements (python)............................(no files to check)Skipped
check python ast.....................................(no files to check)Skipped
ruff check...........................................(no files to check)Skipped
ruff format..........................................(no files to check)Skipped
mypy.................................................(no files to check)Skipped
[add-doc-for-pre-commit 4faac87e] docs: add sentence on installing pre-commit
 1 file changed, 4 insertions(+)
</code></pre>
</details>
