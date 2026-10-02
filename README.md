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

## Mise en place de l'environnement de développement

Ce dépôt ne contient pas de `pyproject.toml` ni de fichier de dépendances pour les imports des DAGs (voir la section « Linting »). L'environnement de développement sert uniquement aux **outils de qualité** (lint, format, tests, pre-commit) et est isolé du reste.

### Prérequis

- **Python 3.12** : c'est la version utilisée par la stack Airflow qui exécute ces DAGs (cf. `Dockerfile` de `apache/airflow`). Une version proche fonctionne en général, mais 3.12 est recommandé et celle que l'on vise.
- L'outil [`uv`](https://docs.astral.sh/uv/) est recommandé mais **pas obligatoire** : une procédure sans `uv` est fournie ci-dessous.

### Avec `uv` (recommandé, reproductible)

Le fichier `.python-version` à la racine fixe la version de Python (3.12) ; `uv` le lit automatiquement.

```
uv venv
uv pip install -r dev-requirements.txt
pre-commit install
```

### Lorsque `uv venv` ne peut pas s'exécuter dans le répertoire courant

Selon l'environnement (machine, VM, montage réseau type SSHFS, conteneur isolé…), la création d'un environnement virtuel **dans le répertoire du dépôt** peut échouer (ex. `Operation not permitted` au moment de résoudre l'interpréteur `.venv/bin/python3`). Les opérations sur les fichiers (création, suppression) peuvent aussi être plus lentes selon le système de fichiers (SSHFS par exemple).

Dans ce cas, on crée l'environnement dans un emplacement **local et persistant** (par exemple `$HOME`, hors du dépôt), puis on l'active afin que les commandes du README s'appliquent normalement à celui-ci :

```
uv venv --python 3.12 "$HOME/.venvs/datagouvfr_data_pipelines"
source "$HOME/.venvs/datagouvfr_data_pipelines/bin/activate"
```

Après `source …/activate`, les commandes `uv pip install -r dev-requirements.txt`, `pre-commit`, `pytest` et `ruff` utilisent l'environnement ainsi activé, sans re-téléchargement:

```
uv pip install -r dev-requirements.txt
pre-commit install
```

Pour l'utiliser ensuite **sans activation manuelle**, exporter de façon persistante (ex. dans le fichier de démarrage du shell) ; `uv` s'appuie alors sur `VIRTUAL_ENV` à chaque nouveau shell :

```sh
export VIRTUAL_ENV="$HOME/.venvs/datagouvfr_data_pipelines"
export PATH="$VIRTUAL_ENV/bin:$PATH"
```

> Note : l'environnement est créé **une seule fois** dans `$HOME` ; il persiste entre les redémarrages (contrairement à un répertoire temporaire type `/tmp`).

### Sans `uv`

```
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r dev-requirements.txt
pre-commit install
```

(Remplacer `python3.12` par l'interpréteur 3.12 de la machine ; `pyenv` lit aussi `.python-version`.)

### Notes importantes

- **`dev-requirements.txt` ne contient pas les dépendances d'exécution des DAGs** : il fournit uniquement les outils de développement (lint, format, tests, pre-commit). L'installation des imports utilisés par les DAGs n'est pas couverte ici. Les dépendances sont dans le fichier [`requirements.txt`](https://github.com/datagouv/data-engineering-stack/blob/master/requirements.txt) du dépôt `data-engineering-stack`.
- **Il est possible de travailler avec des versions différentes** (pas de VM, autre version de Python, `uv` absent) : le fichier `.python-version` est une indication, pas une contrainte. Le seul point de cohérence obligatoire entre contributeurs/trices est porté par pre-commit, qui installe des versions **exactes** de `ruff` et `mypy` (voir `.pre-commit-config.yaml`).
- L'environnement créé (`.venv/`) est ignoré par git ; ne pas le committer.

## Linting

Ce dépôt est formaté avec [`ruff`](https://docs.astral.sh/ruff/) en [configuration par défaut](https://docs.astral.sh/ruff/configuration/), avant de commit :

```
ruff check --fix .
ruff format .
```

`ruff` et `mypy` sont **épinglés à des versions exactes** dans `.pre-commit-config.yaml` (et reflétées dans `dev-requirements.txt`). Pour que tout le monde obtienne le même résultat, lancer de préférence les outils via pre-commit ; il installe lui-même les bonnes versions dans un environnement isolé :

```
pre-commit run --all-files
```

ou laisser pre-commit s'exécuter automatiquement au `commit` grâce au hook installé à l'étape « Mise en place ».

Après avoir cloné ce dépôt, ne pas oublier pas d'installer https://pre-commit.com/ et d'exécuter `pre-commit install` pour installer les scripts de hooks git.

<details><summary>Exemple de sortie de pre-commit</summary>
Lors du premier `commit` après l'installation, pre-commit devrait s'exécuter :

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

## Tests

Pour lancer les tests, il faut d'abord s'assurer d'avoir installé les dépendances nécessaires (dans l'environnement virtuel) :

```shell
uv pip install -r verticales/simplifions/tests/test-requirements.txt
```

Lancer les tests de verticales/simplifions :

```shell
pytest verticales/simplifions/tests/ -s -v
```