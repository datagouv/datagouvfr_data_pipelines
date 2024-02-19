from datetime import datetime, timedelta
import pandas as pd
import requests
from langdetect import detect
import os
import random
import aiohttp
import asyncio
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MATTERMOST_DATAGOUV_CURATION,
    MATTERMOST_DATAGOUV_EDITO,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query, SPAM_WORDS

DAG_NAME = "dgv_bizdev"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
datagouv_api_url = 'https://www.data.gouv.fr/api/1/'
api_metrics_url = "https://metric-api.data.gouv.fr/"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


async def url_error(url, session, method="head"):
    agents = [
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/99.0.4844.83 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/99.0.4844.51 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
        )
    ]
    try:
        _method = getattr(session, method)
        async with _method(
            url,
            timeout=15,
            allow_redirects=True,
            headers={"User-Agent": random.choice(agents)}
        ) as r:
            if r.status in [
                301, 302, 303,
                401, 403, 404, 405,
                500
            ]:
                if method == "head":
                    # HEAD might not be allowed or correctly implemented, trying with GET
                    return await url_error(url, session, method="get")
            r.raise_for_status()
        return False
    except Exception as e:
        return e.status if hasattr(e, "status") else str(e)


async def crawl_reuses(reuses):
    async with aiohttp.ClientSession() as session:
        url_errors = await asyncio.gather(*[url_error(reuse["url"], session) for reuse in reuses])

    unavailable_reuses = [
        (reuse, error) for reuse, error in zip(reuses, url_errors)
        if error
    ]
    return unavailable_reuses


def get_unavailable_reuses():
    print("Fetching reuse list from https://www.data.gouv.fr/api/1/reuses/")
    reuses = list(
        get_all_from_api_query(
            "https://www.data.gouv.fr/api/1/reuses/?page_size=100&sort=-created",
            mask="data{id,url}"
        )
    )
    # gather on the whole reuse list is inconsistent (some unavailable reuses do not
    # appear), having a smaller batch size provides better results and keeps the duration
    # acceptable (compared to synchronous process) 
    batch_size = 50
    print(f"Checking {len(reuses)} reuses with batch size of", batch_size)
    unavailable_reuses = []
    for k in range(len(reuses) // batch_size + 1):
        batch = reuses[k * batch_size:min((k + 1) * batch_size, len(reuses))]
        current = len(unavailable_reuses)
        unavailable_reuses += asyncio.run(crawl_reuses(batch))
        print(f"Batch n°{k + 1} errors: ", len(unavailable_reuses) - current)
    print("All errors:", len(unavailable_reuses))
    return unavailable_reuses


async def classify_user(user):
    try:
        if user['about']:
            if any([sus in user['about'] for sus in ['http', 'www.']]):
                return {
                    'type': 'users',
                    'url': f"https://www.data.gouv.fr/fr/admin/user/{user['id']}/",
                    'name_or_title': None,
                    'creator': None,
                    'id': user['id'],
                    'spam_word': 'web address',
                    'nb_datasets_and_reuses': user['metrics']['datasets'] + user['metrics']['reuses'],
                    'about': user['about'][:1000],
                }
            elif detect(user['about']) != 'fr':
                return {
                    'type': 'users',
                    'url': f"https://www.data.gouv.fr/fr/admin/user/{user['id']}/",
                    'name_or_title': None,
                    'creator': None,
                    'id': user['id'],
                    'spam_word': 'language',
                    'nb_datasets_and_reuses': user['metrics']['datasets'] + user['metrics']['reuses'],
                    'about': user['about'][:1000],
                }
            else:
                return None
    except:
        return {
            'type': 'users',
            'url': f"https://www.data.gouv.fr/fr/admin/user/{user['id']}/",
            'name_or_title': None,
            'creator': None,
            'id': user['id'],
            'spam_word': 'suspect error',
            'nb_datasets_and_reuses': user['metrics']['datasets'] + user['metrics']['reuses'],
            'about': user['about'][:1000],
        }


async def get_suspect_users():
    users = get_all_from_api_query(datagouv_api_url + 'users/', mask="data{id,about,metrics}")
    tasks = [asyncio.create_task(classify_user(k)) for k in users]
    results = await asyncio.gather(*tasks)
    return results


def create_all_tables():
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
    last_month = last_day_of_last_month.strftime("%Y-%m")

    # Tops tous les 1ers du mois
    if today.day == 1:
        # Top 50 des orga qui ont publié le plus de jeux de données
        print('Top 50 des orga qui ont publié le plus de jeux de données')
        data = get_all_from_api_query(
            'https://www.data.gouv.fr/api/1/datasets/?sort=-created',
            mask="data{organization{id,name},internal{created_at_internal}}"
        )
        threshold = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        orgas_of_interest = {}
        for d in data:
            if d['internal']['created_at_internal'] < threshold:
                break
            if d.get('organization', False):
                orgas_of_interest.setdefault(
                    d['organization']['id'], {
                        'name': d['organization']['name'],
                        'url': f"www.data.gouv.fr/fr/organizations/{d['organization']['id']}/",
                        'publications': 0,
                    }
                )
                orgas_of_interest[d['organization']['id']]['publications'] += 1
        orgas_of_interest = {
            k: v for k, v in sorted(orgas_of_interest.items(), key=lambda x: -x[1]['publications'])
        }
        df = pd.DataFrame(orgas_of_interest.values(), index=orgas_of_interest.keys())
        df[:50].to_csv(DATADIR + 'top50_orgas_most_publications_30_days.csv', index=False)

        # Top 50 des orga les plus visitées et avec le plus de ressources téléchargées
        # aka le plus de visites sur tous les datasets de l'orga
        print('Top 50 des orga les plus visitées et avec le plus de ressources téléchargées')
        data = get_all_from_api_query(
            f'{api_metrics_url}api/organizations/data/?metric_month__exact={last_month}',
            next_page='links.next'
        )
        orga_visited = {
            d['organization_id']: {
                'monthly_visit_dataset': d['monthly_visit_dataset'],
                'monthly_download_resource': d['monthly_download_resource']
            }
            for d in data
            if d['monthly_visit_dataset'] or d['monthly_download_resource']
        }
        tmp = list({
            k: v for k, v in sorted(
                orga_visited.items(),
                key=lambda x: -x[1]['monthly_visit_dataset'] if x[1]['monthly_visit_dataset'] else 0
            )
        }.keys())[:50]
        tmp2 = list({
            k: v for k, v in sorted(
                orga_visited.items(),
                key=lambda x: -x[1]['monthly_download_resource'] if x[1]['monthly_download_resource'] else 0
            )
        }.keys())[:50]
        orga_visited = {k: orga_visited[k] for k in orga_visited if k in tmp or k in tmp2}
        for k in orga_visited:
            r = requests.get(datagouv_api_url + 'organizations/' + k).json()
            orga_visited[k].update({'name': r.get('name', None), 'url': r.get('page', None)})
        df = pd.DataFrame(orga_visited.values(), index=orga_visited.keys())
        df.to_csv(DATADIR + 'top50_orgas_most_visits_last_month.csv', index=False)

        # Top 50 des JDD les plus visités
        print('Top 50 des JDD les plus visités')
        data = get_all_from_api_query(
            f'{api_metrics_url}api/datasets/data/?metric_month__exact={last_month}&monthly_visit__sort=desc',
            next_page='links.next'
        )
        datasets_visited = {}
        for k in range(50):
            d = next(data)
            datasets_visited.update({d['dataset_id']: {'monthly_visit': d['monthly_visit']}})
        datasets_visited = {
            k: v for k, v in sorted(datasets_visited.items(), key=lambda x: -x[1]['monthly_visit'])
        }
        for k in datasets_visited:
            r = requests.get(datagouv_api_url + 'datasets/' + k).json()
            datasets_visited[k].update({
                'title': r.get('title', None),
                'url': r.get('page', None),
                'organization_or_owner': (
                    r['organization'].get('name', None) if r.get('organization', None)
                    else r['owner'].get('slug', None) if r.get('owner', None) else None
                )
            })
        df = pd.DataFrame(datasets_visited.values(), index=datasets_visited.keys())
        df.to_csv(DATADIR + 'top50_datasets_most_visits_last_month.csv', index=False)

        # Top 50 des ressources les plus téléchargées
        print('Top 50 des ressources les plus téléchargées')
        data = get_all_from_api_query(
            (
                f'{api_metrics_url}api/resources/data/?metric_month__exact={last_month}'
                '&monthly_download_resource__sort=desc'
            ),
            next_page='links.next'
        )
        resources_downloaded = {}
        while len(resources_downloaded) < 50:
            d = next(data)
            if d['monthly_download_resource']:
                r = requests.get(
                    f"https://www.data.gouv.fr/api/2/datasets/resources/{d['resource_id']}/"
                ).json()
                d['dataset_id'] = r['dataset_id'] if r['dataset_id'] else 'COMMUNAUTARY'
                resource_title = r['resource']['title']
                r2 = {}
                r3 = {}
                if d['dataset_id'] != 'COMMUNAUTARY':
                    r2 = requests.get(f"https://www.data.gouv.fr/api/1/datasets/{d['dataset_id']}/").json()
                    r3 = requests.get(
                        f"{api_metrics_url}api/datasets/data/?metric_month__exact={last_month}"
                        f"&dataset_id__exact={d['dataset_id']}"
                    ).json()
                resources_downloaded.update({
                    f"{d['dataset_id']}.{d['resource_id']}": {
                        'monthly_download_resourced': d['monthly_download_resource'],
                        'resource_title': resource_title,
                        'dataset_url': (
                            f"https://www.data.gouv.fr/fr/datasets/{d['dataset_id']}/"
                            if d['dataset_id'] != 'COMMUNAUTARY' else None
                        ),
                        'dataset_title': r2.get('title', None),
                        'organization_or_owner': (
                            r2['organization'].get('name', None) if r2.get('organization', None)
                            else r2['owner'].get('slug', None) if r2.get('owner', None) else None
                        ),
                        'dataset_total_resource_visits': (
                            r3['data'][0].get('monthly_download_resource', None)
                            if d['dataset_id'] != 'COMMUNAUTARY' and r3['data'] else None
                        )
                    }
                })
        resources_downloaded = {
            k: v for k, v in sorted(
                resources_downloaded.items(),
                key=lambda x: -x[1]['monthly_download_resourced']
            )
        }
        df = pd.DataFrame(resources_downloaded.values(), index=resources_downloaded.keys())
        df = df.sort_values(['dataset_total_resource_visits', 'monthly_download_resourced'], ascending=False)
        df.to_csv(
            DATADIR + 'top50_resources_most_downloads_last_month.csv',
            float_format="%.0f",
            index=False
        )

        # Top 50 des réutilisations les plus visitées
        print('Top 50 des réutilisations les plus visitées')
        data = get_all_from_api_query(
            f'{api_metrics_url}api/reuses/data/?metric_month__exact={last_month}&monthly_visit__sort=desc',
            next_page='links.next',
            ignore_errors=True
        )
        reuses_visited = {}
        while len(reuses_visited) < 50:
            d = next(data)
            if d['monthly_visit']:
                reuses_visited.update({
                    d['reuse_id']: {'monthly_visit': d['monthly_visit']}
                })
        reuses_visited = {
            k: v for k, v in sorted(reuses_visited.items(), key=lambda x: -x[1]['monthly_visit'])
        }
        for k in reuses_visited:
            r = requests.get(datagouv_api_url + 'reuses/' + k).json()
            reuses_visited[k].update({
                'title': r.get('title', None),
                'url': r.get('page', None),
                'creator': (
                    r['organization'].get('name', None) if r.get('organization', None)
                    else r['owner'].get('slug', None) if r.get('owner', None) else None
                )
            })
        df = pd.DataFrame(reuses_visited.values(), index=reuses_visited.keys())
        df.to_csv(DATADIR + 'top50_reuses_most_visits_last_month.csv', index=False)

        # Top 50 des JDD les plus discutés
        print('Top 50 des JDD les plus discutés')
        data = get_all_from_api_query(
            datagouv_api_url + 'discussions/?sort=-created',
            mask="data{created,subject{id,class}}"
        )
        threshold = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        discussions_of_interest = {}
        for d in data:
            if d['created'] < threshold:
                break
            if d['subject']['class'] == 'Dataset':
                tmp = requests.get(datagouv_api_url + f"datasets/{d['subject']['id']}").json()
                organization_or_owner = (
                    tmp.get('organization', {}).get('name', None) if tmp.get('organization', None)
                    else tmp.get('owner', {}).get('slug', None)
                )
                discussions_of_interest.setdefault(
                    d['subject']['id'], {
                        'discussions_created': 0,
                        'dataset_id': d['subject']['id'],
                        'dataset_url': f"https://www.data.gouv.fr/fr/datasets/{d['subject']['id']}",
                        'organization_or_owner': organization_or_owner
                    }
                )
                discussions_of_interest[d['subject']['id']]['discussions_created'] += 1
        discussions_of_interest = {
            k: v for k, v in sorted(
                discussions_of_interest.items(),
                key=lambda x: -x[1]['discussions_created']
            )
        }
        df = pd.DataFrame(discussions_of_interest.values(), index=discussions_of_interest.keys())
        df.to_csv(DATADIR + 'top50_orgas_most_discussions_30_days.csv', index=False)

    # Reuses down, JDD vides et spams tous les lundis
    if today.weekday() == 0:
        # Reuses inaccessibles
        print('Reuses inaccessibles')
        unavailable_reuses = get_unavailable_reuses()
        restr_reuses = {
            d[0]['id']: {'error': d[1]} for d in unavailable_reuses
        }
        for rid in restr_reuses:
            data = get_all_from_api_query(
                f'{api_metrics_url}api/reuses/data/?metric_month__exact={last_month}&reuse_id__exact={rid}',
                next_page='links.next',
                ignore_errors=True
            )
            try:
                d = next(data)
                restr_reuses[rid].update({'monthly_visit': d['monthly_visit']})
            except StopIteration:
                restr_reuses[rid].update({'monthly_visit': 0})

            r = requests.get(datagouv_api_url + 'reuses/' + rid).json()
            restr_reuses[rid].update({
                'title': r.get('title', None),
                'page': r.get('page', None),
                'url': r.get('url', None),
                'creator': (
                    r.get('organization', None).get('name', None) if r.get('organization', None)
                    else r.get('owner', {}).get('slug', None) if r.get('owner', None) else None
                )
            })
        df = pd.DataFrame(restr_reuses.values(), index=restr_reuses.keys())
        df = df.sort_values('monthly_visit', ascending=False)
        df.to_csv(DATADIR + 'all_reuses_most_visits_KO_last_month.csv', float_format="%.0f", index=False)

        # Datasets sans ressources
        print('Datasets sans ressources')
        data = get_all_from_api_query(
            datagouv_api_url + 'datasets',
            mask="data{id,title,organization{name},owner{slug},created_at,resources{id}}"
        )
        empty_datasets = {}
        for d in data:
            if not d['resources']:
                empty_datasets.update({d['id']: {
                    'dataset_id': d['id'],
                    'title': d.get('title', None),
                    'url': 'https://www.data.gouv.fr/fr/admin/dataset/' + d['id'],
                    'organization_or_owner': (
                        d['organization'].get('name', None) if d.get('organization', None)
                        else d['owner'].get('slug', None) if d.get('owner', None) else None
                    ),
                    'created_at': d['created_at'][:10] if d.get('created_at', None) else None
                }})
            # keeping this for now, because it's resource consuming. It'll get better with curation
            if len(empty_datasets) == 1000:
                print('Early stopping')
                break
        print('   > Getting visits...')
        for d in empty_datasets:
            data = get_all_from_api_query(
                (
                    f'{api_metrics_url}api/organizations/data/?metric_month__exact={last_month}'
                    '&dataset_id__exact={d}'
                ),
                next_page='links.next'
            )
            try:
                n = next(data)
                empty_datasets[d].update({'last_month_visits': n['monthly_visit']})
            except:
                empty_datasets[d].update({'last_month_visits': 0})
        df = pd.DataFrame(empty_datasets.values(), index=empty_datasets.keys())
        df = df.sort_values('last_month_visits', ascending=False)
        df.to_csv(DATADIR + 'empty_datasets.csv', float_format="%.0f", index=False)

        # Spam
        print('Spam')
        search_types = [
            'datasets',
            'reuses',
            'organizations',
            'users'
        ]
        spam = []
        for obj in search_types:
            print('   - Starting with', obj)
            for word in SPAM_WORDS:
                data = get_all_from_api_query(
                    datagouv_api_url + f'{obj}/?q={word}',
                    mask="data{badges,organization,owner,id,name,title,metrics,created_at,since}"
                )
                for d in data:
                    should_add = True
                    # si l'objet est ou provient d'une orga certifiée => pas spam
                    if obj != 'users':
                        if obj == 'organization':
                            badges = d.get('badges', [])
                        else:
                            badges = (
                                d['organization'].get('badges', [])
                                if d.get('organization', None) else []
                            )
                        if 'certified' in [badge['kind'] for badge in badges]:
                            should_add = False
                    if should_add:
                        spam.append({
                            'type': obj,
                            'url': f"https://www.data.gouv.fr/fr/admin/{obj[:-1]}/{d['id']}/",
                            'name_or_title': d.get('name', d.get('title', None)),
                            'creator': (
                                d['organization'].get('name', None) if d.get('organization', None)
                                else d['owner'].get('slug', None) if d.get('owner', None) else None
                            ),
                            'created_at': d.get('created_at', d.get('since'))[:10],
                            'id': d['id'],
                            'spam_word': word,
                            'nb_datasets_and_reuses': (
                                None if obj not in ['organizations', 'users']
                                else d['metrics']['datasets'] + d['metrics']['reuses']
                            ),
                        })
        print("Détection d'utilisateurs suspects...")
        suspect_users = asyncio.run(get_suspect_users())
        spam.extend([u for u in suspect_users if u])
        df = pd.DataFrame(spam).drop_duplicates(subset='url')
        df.to_csv(DATADIR + 'objects_with_spam_word.csv', index=False)


def send_tables_to_minio():
    print(os.listdir(DATADIR))
    print('Saving tops as millésimes')
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": file,
                "dest_path": f"bizdev/{datetime.today().strftime('%Y-%m-%d')}/",
                "dest_name": file,
            } for file in os.listdir(DATADIR)
            if not any([k in file for k in ['spam', 'KO']])
        ],
    )
    print('Saving KO reuses and spams (erasing previous files)')
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": file,
                "dest_path": "bizdev/",
                "dest_name": file,
            } for file in os.listdir(DATADIR)
            if any([k in file for k in ['spam', 'KO']])
        ],
    )


def publish_mattermost():
    print("Publishing on mattermost")
    list_curation = ["empty", "spam", "KO"]
    curation = [f for f in os.listdir(DATADIR) if any([k in f for k in list_curation])]
    if curation:
        print("   - Files for curation:")
        print(curation)
        message = ":zap: Les rapports bizdev curation sont disponibles :"
        for file in curation:
            url = f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}"
            if any([k in file for k in ['spam', 'KO']]):
                url += f"/bizdev/{file}"
            else:
                url += f"/bizdev/{datetime.today().strftime('%Y-%m-%d')}/{file}"
            message += f"\n - [{file}]"
            message += f"(https://explore.data.gouv.fr/tableau?url={url}) "
            message += f"[⬇️]({url})"
        send_message(message, MATTERMOST_DATAGOUV_CURATION)

    edito = [f for f in os.listdir(DATADIR) if f not in curation]
    if edito:
        print("   - Files for édito:")
        print(edito)
        message = ":zap: Les rapports bizdev édito sont disponibles :"
        for file in edito:
            url = f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}"
            if any([k in file for k in ['spam', 'KO']]):
                url += f"/bizdev/{file}"
            else:
                url += f"/bizdev/{datetime.today().strftime('%Y-%m-%d')}/{file}"
            message += f"\n - [{file}]"
            message += f"(https://explore.data.gouv.fr/tableau?url={url}) "
            message += f"[⬇️]({url})"
        send_message(message, MATTERMOST_DATAGOUV_EDITO)


default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": False}

with DAG(
    dag_id=DAG_NAME,
    # every Monday (for spam, empty JDD and KO reuses) and every 1st day of month for tops
    schedule_interval="0 4 1 * 1",
    start_date=datetime(2023, 10, 15),
    dagrun_timeout=timedelta(minutes=120),
    tags=["curation", "bizdev", "monthly", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    ),

    create_all_tables = PythonOperator(
        task_id="create_all_tables",
        python_callable=create_all_tables
    )

    send_tables_to_minio = PythonOperator(
        task_id="send_tables_to_minio",
        python_callable=send_tables_to_minio
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    create_all_tables.set_upstream(clean_previous_outputs)
    send_tables_to_minio.set_upstream(create_all_tables)
    publish_mattermost.set_upstream(send_tables_to_minio)
