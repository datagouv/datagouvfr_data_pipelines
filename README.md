# Data pipelines data.gouv.fr

This repository contains all the Airflow DAGs of the data.gouv.fr team. The source code used to build the Airflow stack we run is hosted [in this repository](https://github.com/datagouv/data-engineering-stack/).

Its goal is to harmonise data processing practices within the data.gouv.fr team and to gather as many of these processing jobs as possible in a single repository.

These DAGs run several types of data pipelines (these types are reflected in the repository structure):
- **data processing**: these pipelines fetch existing data from data.gouv.fr (or elsewhere) and process it so that it is easier to use (e.g. Sirene geocoding, which geocodes the whole SIRENE database)
- **dgv**: these pipelines are for internal use by the data.gouv.fr team. They are used to monitor the platform's activity
- **schema**: these pipelines are used to maintain the schema.data.gouv.fr website and the related processing.

## Adding a DAG

- open a PR on this repository following its structure (create one subfolder per processing job)
- put your DAGs' parameters in Airflow variables

## Setting up the development environment

Dependencies are declared in [`pyproject.toml`](pyproject.toml) and locked in `uv.lock`:

- `[project.dependencies]`: the runtime dependencies of the DAGs. The list mirrors the `requirements.txt` of the Airflow image ([`data-engineering-stack`](https://github.com/datagouv/data-engineering-stack/)); packages that are not used by this repository are flagged with a comment.
- `[dependency-groups] dev`: development tools (lint, format, tests, pre-commit).

### Prerequisites

- **Python 3.12**: this is the version used by the Airflow stack that runs these DAGs (see the `apache/airflow` `Dockerfile`). A nearby version usually works, but 3.12 is recommended and is the one we target.
- The [`uv`](https://docs.astral.sh/uv/) tool.

### Installation

The `.python-version` file at the root pins the Python version (3.12); `uv` reads it automatically (and downloads the interpreter if needed).

```shell
uv sync
```

`uv sync` creates the `.venv/` environment and installs exactly the versions from `uv.lock`, including runtime dependencies and the `dev` group. Commands are then run with `uv run <command>` (or directly after `source .venv/bin/activate`).

To add or update a dependency, use `uv add <package>` (or `uv add --dev <package>` for a development tool): `pyproject.toml` and `uv.lock` are updated together.

### Important notes

- The only mandatory point of consistency between contributors is enforced by pre-commit, which installs **exact** versions of `ruff` and `mypy` (see `.pre-commit-config.yaml`).
- The created environment (`.venv/`) is ignored by git; do not commit it.

## Linting

This repository is formatted with [`ruff`](https://docs.astral.sh/ruff/) using the [default configuration](https://docs.astral.sh/ruff/configuration/). Before committing:

```shell
uv run ruff check --fix .
uv run ruff format .
```

`ruff` is **pinned to an exact version** in `.pre-commit-config.yaml` (and mirrored in the `dev` group of `pyproject.toml`). `mypy` runs from the project's virtual environment (`local` hook with `language: system`) so that it can see the runtime dependencies and their types; its version is pinned in the `dev` group of `pyproject.toml`.

The configuration lives in the `[tool.ruff]` section of [`pyproject.toml`](pyproject.toml) and is used both by the pre-commit hook and by `ruff check` on the command line, for identical behaviour everywhere. The selected rules, the docstring convention and its known limitations are documented there in comments.

So that everyone gets the same result, preferably run the tools through pre-commit; `ruff` is installed in an isolated environment, while `mypy` uses the project's virtual environment (hence `uv run`):

```shell
uv run pre-commit run --all-files
```

or let pre-commit run automatically on `commit` through the git hook (see "Contributing").


## Tests

To run the tests, first make sure the dependencies are installed (see "Installation"):

```shell
uv sync
```

Run the whole suite with a single command, from the repository root:

```shell
uv run pytest -v
```

`pytest.ini` declares the test folders (`data_processing`, `dgv`, `utils`)
and the `pythonpath` needed for absolute imports. The root `conftest.py`
provides the shared stubs (Airflow, external clients) to all suites.

## Contributing

Before contributing to the repository and opening a PR, you need to install the pre-commit hooks:

```shell
uv run pre-commit install
```

Once this is done, formatting, linting (including import sorting) and type checking are verified automatically before each commit.

<details><summary>Example pre-commit output</summary>
On the first `commit` after installation, pre-commit should run:

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

If pre-commit cannot be used, you must format and lint the code with Ruff, and check types with mypy. Before submitting a contribution, you must either install the pre-commit hook or run these commands manually:

```shell
# Lint (including import sorting) and formatting
uv run ruff check --fix && uv run ruff format

# Type checking (mypy)
uv run mypy .
```

## CI

CI runs on [Circle CI](https://app.circleci.com/projects/github/datagouv/datagouvfr_data_pipelines), see [.circleci/config.yml](./.circleci/config.yml). There are no GitHub Actions. The `lint` job replays the local pre-commit hooks, the `test` job runs the whole suite with `pytest`, and the `dag-imports` job checks that the DAGs import correctly.

On the GitHub repository, GitGuardian also runs to prevent token leaks.
