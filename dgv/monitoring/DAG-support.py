from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from time import sleep
import json
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    SECRET_ZAMMAD_API_URL,
    SECRET_ZAMMAD_API_TOKEN,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.utils import month_year_iter
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query
from datagouvfr_data_pipelines.utils.mattermost import send_message

DAG_NAME = "dgv_support_monitor"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
one_year_ago = datetime.today() - timedelta(days=365)
groups = [
    k + "@" + ".".join(['data', 'gouv', 'fr'])
    for k in ['support', 'ouverture', 'moissonnage', 'certification']
]
api_url = "https://recherche-entreprises.api.gouv.fr/search?q="

minio_open = MinIOClient(bucket='dataeng-open')
minio_destination_folder = "dashboard/"


def get_monthly_tickets(year_month, tags=None, per_page=200):
    session = requests.Session()
    session.headers = {"Authorization": "Bearer " + SECRET_ZAMMAD_API_TOKEN}

    query = f"created_at:[{year_month}-01 TO {year_month}-31]"
    query += f" AND (group.name:{' OR group.name:'.join(groups)})"
    if tags:
        query += f" AND (tags:{' OR tags:'.join(tags)})"
    page = 1
    params = {
        "query": query,
        "page": page,
        "per_page": 200,
    }
    res = session.get(
        f'{SECRET_ZAMMAD_API_URL}tickets/search',
        params=params
    ).json()['tickets_count']
    batch = res
    while batch == per_page:
        page += 1
        params['page'] = page
        batch = session.get(
            f'{SECRET_ZAMMAD_API_URL}tickets/search',
            params=params
        ).json()['tickets_count']
        res += batch
    print(f"{res} tickets found for month {year_month} across {page} pages")
    return res


def get_zammad_tickets(
    ti,
    start_date,
    end_date=datetime.today(),
):
    hs_tags = [
        'HORS-SUJET',
        'RNA',
        # quotes are mandatory if tag has blanks
        '"TITRE DE SEJOUR"',
        'DECES',
        'QUALIOPI',
        'IMPOT',
    ]

    all_tickets = []
    hs_tickets = []
    for year, month in month_year_iter(
        start_date.month,
        start_date.year,
        end_date.month,
        end_date.year,
    ):
        all_tickets.append(
            get_monthly_tickets(f"{year}-{'0'*(month<10) + str(month)}")
        )
        hs_tickets.append(
            get_monthly_tickets(f"{year}-{'0'*(month<10) + str(month)}", tags=hs_tags)
        )
    ti.xcom_push(key="all_tickets", value=all_tickets)
    ti.xcom_push(key="hs_tickets", value=hs_tickets)


def fill_url(start, end, site_id, label, **kwargs):
    return (
        f"https://stats.data.gouv.fr/index.php?module=API&format=CSV&idSite={site_id}"
        f"&period=month&date={start},{end}&method=Actions.getPageUrls&label={label}"
        "&filter_limit=100&format_metrics=1&expanded=1&translateColumnNames=1&language=fr"
        "&token_auth=anonymous"
    )


def get_visits(
    ti,
    start_date,
    end_date=datetime.today(),
):
    # url_stats_home_dgv = {
    #     "site_id": 109,
    #     "label": "fr",
    #     "title": "Homepage",
    # }
    url_stats_support = {
        "site_id": 176,
        "label": "%40%252Findex",
        "title": "support",
    }
    for k in [
        # not taking the stats from the homepage, no variation
        # url_stats_home_dgv,
        url_stats_support
    ]:
        r = requests.get(fill_url(
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            **k
        ))
        df = pd.read_csv(StringIO(r.text))
        months = df['Date'].to_list()
        vues = df['Vues de page uniques'].to_list()
        ti.xcom_push(key=k['title'], value=vues)
    ti.xcom_push(key='months', value=months)


def gather_and_upload(
    ti,
):
    all_tickets = ti.xcom_pull(key="all_tickets", task_ids="get_zammad_tickets")
    hs_ticket = ti.xcom_pull(key="hs_tickets", task_ids="get_zammad_tickets")
    # homepage = ti.xcom_pull(key="homepage", task_ids="get_visits")
    support = ti.xcom_pull(key="support", task_ids="get_visits")
    months = ti.xcom_pull(key="months", task_ids="get_visits")

    stats = pd.DataFrame({
        # 'Homepage': homepage,
        'Page support': support,
        'Ouverture de ticket': all_tickets,
        'Ticket hors-sujet': hs_ticket,
    }, index=months).T
    # removing current month from stats
    stats = stats[stats.columns[:-1]]
    stats.to_csv(DATADIR + 'stats_support.csv')

    # sending to minio
    minio_open.send_files(
        list_files=[
            {
                "source_path": DATADIR,
                "source_name": 'stats_support.csv',
                "dest_path": minio_destination_folder,
                "dest_name": 'stats_support.csv',
            }
        ],
        ignore_airflow_env=True,
    )


def is_certified(badges):
    for b in badges:
        if b['kind'] == 'certified':
            return True
    return False


def is_SP_or_CT(siret, session):
    issue = None
    if siret is None:
        return False, issue
    try:
        r = session.get(api_url + siret).json()
    except:
        sleep(1)
        r = session.get(api_url + siret).json()
    if len(r['results']) == 0:
        print('No match for: ', siret)
        issue = "pas de correspondance : " + siret
        return False, issue
    if len(r['results']) > 1:
        print('Ambiguous: ', siret)
        issue = "SIRET ambigu : " + siret
    complements = r['results'][0]['complements']
    return complements['collectivite_territoriale'] or complements['est_service_public'], issue


def get_and_upload_certification():
    session = requests.Session()
    orgas = get_all_from_api_query(
        'https://www.data.gouv.fr/api/1/organizations',
        mask='data{id,badges,business_number_id}'
    )
    certified = []
    SP_or_CT = []
    issues = []
    for o in orgas:
        if is_certified(o['badges']):
            certified.append(o['id'])
        it_is, issue = is_SP_or_CT(o['business_number_id'], session)
        if it_is:
            SP_or_CT.append(o['id'])
        if issue:
            issues.append({o['id']: issue})
    with open(DATADIR + 'certified.json', 'w') as f:
        json.dump(certified, f)
    with open(DATADIR + 'SP_or_CT.json', 'w') as f:
        json.dump(SP_or_CT, f)
    with open(DATADIR + 'issues.json', 'w') as f:
        json.dump(issues, f)

    minio_open.send_files(
        list_files=[
            {
                "source_path": DATADIR,
                "source_name": f,
                "dest_path": minio_destination_folder + datetime.now().strftime("%Y-%m-%d") + '/',
                "dest_name": f,
            } for f in ['certified.json', 'SP_or_CT.json', 'issues.json']
        ],
        ignore_airflow_env=True,
    )


default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": False}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 4 1 * *",
    start_date=datetime(2023, 10, 15),
    dagrun_timeout=timedelta(minutes=30),
    tags=["support", "certification"],
    default_args=default_args,
    catchup=False,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    )

    get_zammad_tickets = PythonOperator(
        task_id="get_zammad_tickets",
        python_callable=get_zammad_tickets,
        op_kwargs={'start_date': one_year_ago},
    )

    get_visits = PythonOperator(
        task_id="get_visits",
        python_callable=get_visits,
        op_kwargs={'start_date': one_year_ago},
    )

    get_and_upload_certification = PythonOperator(
        task_id="get_and_upload_certification",
        python_callable=get_and_upload_certification,
    )

    gather_and_upload = PythonOperator(
        task_id="gather_and_upload",
        python_callable=gather_and_upload,
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=send_message,
        op_kwargs={
            'text': ":bar_chart: Données du dashboard de suivi des indicateurs mises à jour."
        },
    )

    get_zammad_tickets.set_upstream(clean_previous_outputs)
    get_visits.set_upstream(clean_previous_outputs)
    get_and_upload_certification.set_upstream(clean_previous_outputs)

    gather_and_upload.set_upstream(get_zammad_tickets)
    gather_and_upload.set_upstream(get_visits)

    publish_mattermost.set_upstream(gather_and_upload)
    publish_mattermost.set_upstream(get_and_upload_certification)
