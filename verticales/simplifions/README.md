
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

- `verticale_simplifions_v2_production` : Runs once a day
  - On local environment, fills demo.data.gouv.fr
  - On production environment, fills www.data.gouv.fr

- `verticale_simplifions_v2_demo` : Runs once every 30 minutes
  - On local environment, fills demo.data.gouv.fr
  - On production environment, fills demo.data.gouv.fr

But both DAGs use the same code.

## Install in airflow

1. Clone the repo with airflow : [data-engineering-stack](https://github.com/datagouv/data-engineering-stack)
2. Follow its readme to install airflow
3. Clone this repo in the `dags` folder of `data-engineering-stack`
4. Launch airflow with docker, log in, and voilà !

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

### 1. Get the data from grist

The `GristV2Manager` : Fetch data from grist tables `Solutions` and `Cas d'usages`, and the 4 tables of filters.

### 2. Create and update simplifions topics

The `TopicV2Manager` :  Create or update (if changes detected) `cas d'usages` and `solutions` topics from the fetched data. Create the tags for the filters.

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

### ATTENTION

Le venv pour les tests interfère avec airflow. Pour que airflow puisse correctement lister les DAGs, il faut supprimer le dossier venv.

```bash
rm -rf venv/
```

Pour forcer airflow à lister ses DAGs : 

```bash
docker exec -it airflow-demo-test /bin/bash
# Puis dans le conteneur :
airflow dags list
```