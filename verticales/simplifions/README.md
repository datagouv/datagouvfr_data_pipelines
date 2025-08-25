
# Documentation

## verticale_simplifions

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | Ce traitement vise à alimenter la plateforme simplifions.data.gouv.fr à partir du grist source. |
| Fréquence de mise à jour | quotidienne |
| Données sources | grist |
| Données de sorties | topics sous-jacents à simplifions |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |

## Two DAGs for demo & production

Two dags are running to fill demo and production.
But because of how the `local_client` works, both DAGs are filling the demo when running on local.

- `verticale_simplifions_production` : Runs once a day
  - On local environment, fills demo.data.gouv.fr
  - On production environment, fills www.data.gouv.fr

- `verticale_simplifions_demo` : Runs once every 15 minutes
  - On local environment, fills demo.data.gouv.fr
  - On production environment, fills www.data.gouv.fr

But both DAGs use the same code from `simplifions_manager.py`.

## Install in airflow

1. Clone the repo with airflow : [data-engineering-stack](https://github.com/datagouv/data-engineering-stack)
2. Follow its readme to install airflow
3. Clone this repo in the `dags` folder of `data-engineering-stack`
4. Launch airflow with docker, log in, and voilà !

![airflow screenshot]()

## Airflow environment variables

Add these variables in the Airflow UI, under Admin > Variables :

```
GRIST_API_URL=https://grist.numerique.gouv.fr/api/
SECRET_GRIST_API_KEY=<fetch it on your grist account>
DEMO_DATAGOUV_SECRET_API_KEY=<fetch it on your demo.data.gouv.fr account>
# You also need this in dev, but with the demo key. For reasons.
DATAGOUV_SECRET_API_KEY=<same demo key>
```


## What the simplifions DAG does

The `SimplifionsManager` has 3 main functions called by the DAG

### 1. Get the data from grist

Fetch data from grist tables.
Fetch their sub-data too when needed.

### 2. Create and update simplifions topics

Create `cas d'usages` and `solutions` topics from the fetched data.
Create the tags for the filters.

### 3. Update the references between simplifions topics

Because we need all simplifions topics to be created before in order to be able to update all references, we have this third task separated from the second task.

## Tests

1. install and activate the python virtual env

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install the required dependencies:

```bash
pip install -r verticales/simplifions/tests/test-requirements.txt
```

3. Run the tests

```bash
pytest verticales/simplifions/tests/ -s
```