from airflow.models import DAG, Variable
from operators.mattermost import MattermostOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from difflib import SequenceMatcher
from dag_datagouv_data_pipelines.config import (
    MATTERMOST_DATAGOUV_ACTIVITES,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE
)
from dag_datagouv_data_pipelines.utils.datagouv import get_last_items
import requests

DAG_NAME = 'dgv_notification_activite'

TIME_PERIOD = {'hours': 1} 

def check_new(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    # we want everything that happened since this date
    start_date = datetime.now() - timedelta(**TIME_PERIOD)
    end_date = datetime.now()
    items = get_last_items(templates_dict['type'], start_date, end_date)
    # items = get_last_items(templates_dict['type'], start_date)
    ti.xcom_push(key='nb', value=str(len(items))) 
    arr = []
    for item in items:
        mydict = {}
        if("name" in item):
            mydict['name'] = item['name']
        if("title" in item):
            mydict['name'] = item['title']
        if('page' in item):
            mydict['page'] = item['page']
        arr.append(mydict)
    ti.xcom_push(key=templates_dict['type'], value=arr)

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def check_schema(ti):
    nb_datasets = float(ti.xcom_pull(key='nb', task_ids='check_new_datasets'))
    datasets = ti.xcom_pull(key='datasets', task_ids='check_new_datasets')
    result = requests.get('https://schema.data.gouv.fr/schemas/schemas.json')
    catalog = result.json()['schemas']
    if(nb_datasets > 0):
        for item in datasets:
            result = requests.get(item['page'].replace('data.gouv.fr/fr/', 'data.gouv.fr/api/1/'))
            
            if(result.json()['organization'] is not None):
                if('name' in result.json()['organization']):
                    orga = '(Organisation ' + result.json()['organization']['name'] + ')'
                else:
                    orga = ''
            if(result.json()['owner'] is not None):
                if('first_name' in result.json()['owner']):
                    orga = '(Utilisateur ' + result.json()['owner']['first_name'] + ' ' + result.json()['owner']['last_name'] + ')'
                else:
                    orga = ''
            
            try:
                is_schema = False
                for r in result.json()['resources']:
                    print(r)
                    if r['schema']:
                        is_schema = True
                        schema_name = None
                        for s in catalog:
                            if(s['name'] == r['schema']['name']):
                                validata_base_url = "https://api.validata.etalab.studio/validate?schema={schema_url}&url={rurl}"
                                schema_name = s['title']
                                publierDetection = False
                                if 'publish_source' in r['extras']:
                                    if(r['extras']['publish_source'] == 'publier.etalab.studio'):
                                        publierDetection = True
                                if(s['schema_type'] == 'tableschema'):
                                    schema_type = 'tableschema'
                                    result2 = requests.get(validata_base_url.format(schema_url=s['schema_url'], rurl=r['url']))
                                    try:
                                        res = result2.json()['report']['valid']
                                        validata_url = 'https://validata.fr/table-schema?input=url&url=' + r['url'] + \
                                            '&schema_url=' + s['schema_url']
                                    except: 
                                        res = False
                                else:
                                    schema_type = 'other'
                        if not schema_name: schema_name = r['schema']['name']
                        message = ':mega: Nouvelle ressource déclarée appartenant au schéma **{}** {}: \n - [Lien vers le jeu de donnée]({})'.format(
                            schema_name, 
                            orga,
                            item['page'],
                        )
                        if schema_type == 'tableschema':
                            if res:
                                message += '\n - [Ressource valide]({}) :partying_face:'.format(validata_url)
                            else:
                                message += '\n - [Ressource non valide]({}) :weary:'.format(validata_url)
                        if publierDetection:
                            message += '\n - Made with publier.etalab.studio :doge-cool:'
                            
                        publish_mattermost = MattermostOperator(
                            task_id="publish_result",
                            mattermost_endpoint=MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
                            text=message
                        )
                        publish_mattermost.execute(dict()) 

                if not is_schema:
                    schemas = [schema['title'] for schema in catalog]
                    best_score = 0
                    schema_title = ''
                    for schema in schemas:
                        score = similar(schema,item['name'])
                        if score > best_score:
                            best_score = score
                            schema_title = schema
                    if best_score > 0.6:
                        message = ':mega: Nouveau jeu de donnée suspecté d\'appartenir au schéma **{}** {}: \n - [{}]({})'.format(
                            schema_title, 
                            orga,
                            item['name'],
                            item['page'],
                        )
                        publish_mattermost = MattermostOperator(
                            task_id="publish_result",
                            mattermost_endpoint=MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
                            text=message
                        )
                        publish_mattermost.execute(dict()) 
            except:
                pass

def publish_mattermost(ti):
    nb_datasets = float(ti.xcom_pull(key='nb', task_ids='check_new_datasets'))
    datasets = ti.xcom_pull(key='datasets', task_ids='check_new_datasets')
    nb_reuses = float(ti.xcom_pull(key='nb', task_ids='check_new_reuses'))
    reuses = ti.xcom_pull(key='reuses', task_ids='check_new_reuses')
    nb_orgas = float(ti.xcom_pull(key='nb', task_ids='check_new_orgas'))
    orgas = ti.xcom_pull(key='organizations', task_ids='check_new_orgas')

    if(nb_datasets > 0):
        for item in datasets:
            publish_mattermost = MattermostOperator(
                task_id="publish_result",
                mattermost_endpoint=MATTERMOST_DATAGOUV_ACTIVITES,
                text=":loudspeaker: :label: Nouveau **Jeu de données** : *{}* \n\n\n:point_right: {}".format(item['name'],item['page'])
            )
            publish_mattermost.execute(dict()) 
    if(nb_orgas > 0):
        for item in orgas:
            publish_mattermost = MattermostOperator(
                task_id="publish_result",
                mattermost_endpoint=MATTERMOST_DATAGOUV_ACTIVITES,
                text=":loudspeaker: :office: Nouvelle **organisation** : *{}* \n\n\n:point_right: {}".format(item['name'],item['page'])
            )
            publish_mattermost.execute(dict()) 
    if(nb_reuses > 0):
        for item in reuses:
            publish_mattermost = MattermostOperator(
                task_id="publish_result",
                mattermost_endpoint=MATTERMOST_DATAGOUV_ACTIVITES,
                text=":loudspeaker: :art: Nouvelle **réutilisation** : *{}* \n\n\n:point_right: {}".format(item['name'],item['page'])
            )
            publish_mattermost.execute(dict()) 

default_args = {
   'email': ['geoffrey.aldebert@data.gouv.fr'],
   'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='42 * * * *',
    start_date=days_ago(0, hour=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['notification','hourly','datagouv', 'activite', 'schemas'],
    default_args=default_args,
    catchup=False,
) as dag:

    check_new_datasets = PythonOperator(
        task_id="check_new_datasets", 
        python_callable=check_new,
        templates_dict={
            "type": 'datasets'
        },
    )

    check_new_reuses= PythonOperator(
        task_id="check_new_reuses", 
        python_callable=check_new,
        templates_dict={
            "type": 'reuses'
        },
    )

    check_new_orgas= PythonOperator(
        task_id="check_new_orgas", 
        python_callable=check_new,
        templates_dict={
            "type": 'organizations'
        },
    )
    
    publish_mattermost = PythonOperator(
        task_id="publish_mattermost", 
        python_callable=publish_mattermost,
    )

    check_schema = PythonOperator(
        task_id="check_schema", 
        python_callable=check_schema,
    )


    [check_new_datasets, check_new_reuses, check_new_orgas] >> publish_mattermost >> check_schema
