# Data pipelines data.gouv.fr

Ce dépôt contient l'ensemble des DAGs Airflow de l'équipe data.gouv.fr. Le code source permettant de générer la stack airflow que nous utilisons est hébergé [sur ce dépôt](https://github.com/datagouv/data-engineering-stack/).

Il a pour objectif d'harmoniser les pratiques de traitements de données dans l'équipe data.gouv.fr et de répertorier au sein d'un même dépôt le maximum de ces traitements.

Ces dags permettent de faire tourner des pipelines de données de différents types (ces types se reflètent dans la structure du dépôt) :
- **data processing** : ces pipelines récupèrent des données existantes sur data.gouv.fr (ou ailleurs) et les traitent pour qu'ils soient plus facilement utilisables (ex: géocodage Sirene qui géocode l'ensemble de la base SIRENE)
- **dgv** : ces pipelines sont à usage interne de l'équipe data.gouv.fr. Ils permettent de monitorer l'activité de la plateforme
- **schema** : ces pipelines sont utilisés pour maintenir le site schema.data.gouv.fr et les traitements afférents.

## Ajout d'un DAG

- réaliser une PR sur ce dépôt en respectant la structure de celui-ci (créer un sous-dossier par traitement réalisé)
- variabiliser les paramètres de vos DAGs dans des variables Airflow

## Mise en place de l'environnement de développement

Les dépendances sont déclarées dans [`pyproject.toml`](pyproject.toml) et figées dans `uv.lock` :

- `[project.dependencies]` : les dépendances d'exécution des DAGs. La liste reprend le `requirements.txt` de l'image Airflow ([`data-engineering-stack`](https://github.com/datagouv/data-engineering-stack/)) ; les paquets qui ne sont pas utilisés par ce dépôt y sont signalés en commentaire.
- `[dependency-groups] dev` : les outils de développement (lint, format, tests, pre-commit).

### Prérequis

- **Python 3.12** : c'est la version utilisée par la stack Airflow qui exécute ces DAGs (cf. `Dockerfile` de `apache/airflow`). Une version proche fonctionne en général, mais 3.12 est recommandé et celle que l'on vise.
- L'outil [`uv`](https://docs.astral.sh/uv/).

### Installation

Le fichier `.python-version` à la racine fixe la version de Python (3.12) ; `uv` le lit automatiquement (et télécharge l'interpréteur si besoin).

```shell
uv sync
```

`uv sync` crée l'environnement `.venv/` et y installe exactement les versions de `uv.lock`, dépendances d'exécution et groupe `dev` compris. Les commandes se lancent ensuite avec `uv run <commande>` (ou directement après `source .venv/bin/activate`).

Pour ajouter ou mettre à jour une dépendance, utiliser `uv add <paquet>` (ou `uv add --dev <paquet>` pour un outil de développement) : `pyproject.toml` et `uv.lock` sont mis à jour ensemble.

### Notes importantes

- Le seul point de cohérence obligatoire entre contributeurs/trices est porté par pre-commit, qui installe des versions **exactes** de `ruff` et `mypy` (voir `.pre-commit-config.yaml`).
- L'environnement créé (`.venv/`) est ignoré par git ; ne pas le committer.

## Linting

Ce dépôt est formaté avec [`ruff`](https://docs.astral.sh/ruff/) en [configuration par défaut](https://docs.astral.sh/ruff/configuration/), avant de commit :

```shell
uv run ruff check --fix .
uv run ruff format .
```

`ruff` est **épinglé à une version exacte** dans `.pre-commit-config.yaml` (et reflété dans le groupe `dev` de `pyproject.toml`). `mypy` s'exécute depuis l'environnement virtuel du projet (hook `local` avec `language: system`) afin de voir les dépendances d'exécution et leurs types ; sa version est épinglée dans le groupe `dev` de `pyproject.toml`.

La configuration vit dans la section `[tool.ruff]` de [`pyproject.toml`](pyproject.toml), et est utilisée à la fois par le hook de pre-commit et par `ruff check` en ligne de commande, pour un comportement identique partout. Les règles sélectionnées, la convention de docstrings et ses limites connues y sont commentées.

Pour que tout le monde obtienne le même résultat, lancer de préférence les outils via pre-commit ; `ruff` est installé dans un environnement isolé, tandis que `mypy` utilise l'environnement virtuel du projet (d'où `uv run`) :

```shell
uv run pre-commit run --all-files
```

ou laisser pre-commit s'exécuter automatiquement au `commit` grâce au hook git (voir « Contribution »).


## Tests

Pour lancer les tests, il faut d'abord s'assurer d'avoir installé les dépendances (voir « Installation ») :

```shell
uv sync
```

Lancer toute la suite avec une seule commande, depuis la racine du dépôt :

```shell
uv run pytest -v
```

`pytest.ini` déclare les dossiers de tests (`data_processing`, `dgv`, `utils`)
et le `pythonpath` nécessaire aux imports absolus. Le `conftest.py` racine
fournit les stubs communs (Airflow, clients externes) à toutes les suites.

## Contribution

Avant de contribuer au dépôt et d'ouvrir une PR, il faut initialiser les hooks pre-commit :

```shell
uv run pre-commit install
```

Une fois cela fait, le formatage, le lint (tri des imports compris) et le typage sont vérifiés automatiquement avant chaque commit.

<details><summary>Exemple de sortie de pre-commit</summary>
Lors du premier `commit` après l'installation, pre-commit devrait s'exécuter :

<pre><code>
(.venv) ➜  datagouvfr_data_pipelines git:(add-doc-for-pre-commit) ✗ gcmsg "docs: add sentence on installing pre-commit"
[INFO] Initializing environment for https://github.com/pre-commit/pre-commit-hooks.
[INFO] Initializing environment for https://github.com/astral-sh/ruff-pre-commit.
[INFO] Installing environment for https://github.com/pre-commit/pre-commit-hooks.
[INFO] Once installed this environment will be reused.
[INFO] This may take a few minutes...
[INFO] Installing environment for https://github.com/astral-sh/ruff-pre-commit.
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

Si pre-commit ne peut pas être utilisé, il faut formater et linter le code avec Ruff, et vérifier le typage avec mypy. Avant de soumettre une contribution, il est obligatoire soit d'installer le hook pre-commit, soit de lancer ces commandes à la main :

```shell
# Lint (tri des imports compris) et formatage
uv run ruff check --fix && uv run ruff format

# Vérification du typage (mypy)
uv run mypy .
```

## CI

La CI utilise [Circle CI](https://app.circleci.com/projects/github/datagouv/datagouvfr_data_pipelines), voir [.circleci/config.yml](./.circleci/config.yml). Pas de GitHub Actions. Le job `lint` rejoue les hooks pre-commit locaux, le job `test` lance toute la suite avec `pytest`, et le job `dag-imports` vérifie que les DAGs s'importent correctement.

Sur le dépôt GitHub, GitGuardian tourne aussi pour éviter que des tokens fuitent.
