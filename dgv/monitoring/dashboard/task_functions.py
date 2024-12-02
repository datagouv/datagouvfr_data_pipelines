from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from time import sleep
import json
import numpy as np

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    SECRET_ZAMMAD_API_URL,
    SECRET_ZAMMAD_API_TOKEN,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.utils import month_year_iter
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query

DAG_NAME = "dgv_dashboard"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
one_year_ago = datetime.today() - timedelta(days=365)
groups = [
    k + "@data.gouv.fr"
    for k in ['support', 'ouverture', 'moissonnage', 'certification']
]
entreprises_api_url = "https://recherche-entreprises.api.gouv.fr/search?q="
# rate limiting of 7 requests/second
rate_limiting_delay = 1 / 7

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
    months = []
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
        months.append(f"{year}-{'0'*(month<10) + str(month)}")
    ti.xcom_push(key="all_tickets", value=all_tickets)
    ti.xcom_push(key="hs_tickets", value=hs_tickets)
    ti.xcom_push(key="months", value=months)


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
        vues = df['Vues de page uniques'].to_list()
        ti.xcom_push(key=k['title'], value=vues)


def gather_and_upload(
    ti,
):
    all_tickets = ti.xcom_pull(key="all_tickets", task_ids="get_zammad_tickets")
    hs_ticket = ti.xcom_pull(key="hs_tickets", task_ids="get_zammad_tickets")
    months = ti.xcom_pull(key="months", task_ids="get_zammad_tickets")
    # homepage = ti.xcom_pull(key="homepage", task_ids="get_visits")
    support = ti.xcom_pull(key="support", task_ids="get_visits")

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
        sleep(rate_limiting_delay)
        r = session.get(entreprises_api_url + siret).json()
    except:
        print('Sleeping a bit more')
        sleep(1)
        r = session.get(entreprises_api_url + siret).json()
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
        json.dump(certified, f, indent=4)
    with open(DATADIR + 'SP_or_CT.json', 'w') as f:
        json.dump(SP_or_CT, f, indent=4)
    with open(DATADIR + 'issues.json', 'w') as f:
        json.dump(issues, f, indent=4)

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


def get_and_upload_reuses_down():
    client = MinIOClient(bucket="data-pipeline-open")
    # getting latest data
    df = pd.read_csv(StringIO(
        client.get_file_content("prod/bizdev/all_reuses_most_visits_KO_last_month.csv")
    ))
    stats = pd.DataFrame(
        df['error'].apply(lambda x: x if x == "404" else 'Autre erreur').value_counts()
    ).T
    stats['Date'] = [datetime.now().strftime('%Y-%m-%d')]
    stats['Total'] = [
        requests.get(
            'https://www.data.gouv.fr/api/1/reuses/',
            headers={"X-fields": "total"},
        ).json()['total']
    ]

    # getting historical data
    output_file_name = "stats_reuses_down.csv"
    hist = pd.read_csv(StringIO(
        minio_open.get_file_content(minio_destination_folder + output_file_name)
    ))
    start_len = len(hist)
    hist = pd.concat([hist, stats]).drop_duplicates('Date')
    # just in case
    assert start_len <= len(hist)
    hist.to_csv(DATADIR + output_file_name, index=False)
    minio_open.send_files(
        list_files=[
            {
                "source_path": DATADIR,
                "source_name": output_file_name,
                "dest_path": minio_destination_folder,
                "dest_name": output_file_name,
            }
        ],
        ignore_airflow_env=True,
    )


def get_catalog_stats():
    datasets = []
    resources = []
    crawler = get_all_from_api_query(
        'https://www.data.gouv.fr/api/1/datasets/',
        mask='data{id,harvest,quality,tags,resources{id,format,type}}'
    )
    processed = 0
    for c in crawler:
        datasets.append([c['id'], bool(c['harvest']), c['quality'], c['tags']])
        for r in c['resources']:
            tmp = {k: r[k] for k in ['id', 'format', 'type']}
            tmp['hvd'] = 'hvd' in c['tags']
            resources.append(tmp)
        processed += 1
        if processed % 1000 == 0:
            print(f'> {processed} datasets processed')

    # getting datasets quality and count
    cats = list(max(datasets, key=lambda x: len(x[2]))[2].keys())
    dataset_quality = {
        k: {c: [] for c in cats} for k in ['all', 'harvested', 'local', 'hvd']
    }
    dataset_quality.update({'count': {k: 0 for k in ['all', 'harvested', 'local', 'hvd']}})
    for d in datasets:
        dataset_quality['count']['all'] += 1
        dataset_quality['count']['harvested' if d[1] else 'local'] += 1
        if 'hvd' in d[3]:
            dataset_quality['count']['hvd'] += 1
        for c in cats:
            dataset_quality['all'][c].append(d[2].get(c, False) or False)
            dataset_quality['harvested' if d[1] else 'local'][c].append(d[2].get(c, False) or False)
            if 'hvd' in d[3]:
                dataset_quality['hvd'][c].append(d[2].get(c, False) or False)
    for k in dataset_quality.keys():
        for c in dataset_quality[k]:
            dataset_quality[k][c] = np.mean(dataset_quality[k][c])
    dataset_quality = {datetime.now().strftime('%Y-%m-%d'): dataset_quality}

    # getting resources types and formats
    resources_stats = {'all': {}, 'hvd': {}}
    for r in resources:
        if r['format'] not in resources_stats['all']:
            resources_stats['all'][r['format']] = 0
        resources_stats['all'][r['format']] += 1
        if r['hvd']:
            if r['format'] not in resources_stats['hvd']:
                resources_stats['hvd'][r['format']] = 0
            resources_stats['hvd'][r['format']] += 1

        if r['type'] not in resources_stats:
            resources_stats[r['type']] = {}
        if r['format'] not in resources_stats[r['type']]:
            resources_stats[r['type']][r['format']] = 0
        resources_stats[r['type']][r['format']] += 1

    resources_stats = {datetime.now().strftime('%Y-%m-%d'): resources_stats}

    hist_dq = json.loads(
        minio_open.get_file_content(minio_destination_folder + 'datasets_quality.json')
    )
    hist_dq.update(dataset_quality)
    with open(DATADIR + 'datasets_quality.json', 'w') as f:
        json.dump(hist_dq, f, indent=4)

    hist_rs = json.loads(
        minio_open.get_file_content(minio_destination_folder + 'resources_stats.json')
    )
    hist_rs.update(resources_stats)
    with open(DATADIR + 'resources_stats.json', 'w') as f:
        json.dump(hist_rs, f, indent=4)

    minio_open.send_files(
        list_files=[
            {
                "source_path": DATADIR,
                "source_name": output_file_name,
                "dest_path": minio_destination_folder,
                "dest_name": output_file_name,
            }
            for output_file_name in [
                'resources_stats.json',
                'datasets_quality.json'
            ]
        ],
        ignore_airflow_env=True,
    )


def get_hvd_dataservices_stats():
    crawler = get_all_from_api_query(
        'https://www.data.gouv.fr/api/1/dataservices/?tags=hvd'
    )
    count = 0
    # we can add more fields to monitor later
    of_interest = {
        'base_api_url': 0,
        'contact_point': 0,
        'description': 0,
        'endpoint_description_url': 0,
        'license': 0,
    }
    for c in crawler:
        count += 1
        for i in of_interest:
            if i != 'license':
                of_interest[i] += c[i] is not None
            # for now, as license is not None when not specified
            else:
                of_interest[i] += c[i] != "notspecified"

    dataservices_stats = {
        datetime.now().strftime('%Y-%m-%d'): {
            "metrics": of_interest,
            "count": count,
        },
    }

    hist_ds = json.loads(
        minio_open.get_file_content(minio_destination_folder + 'hvd_dataservices_quality.json')
    )
    hist_ds.update(dataservices_stats)
    with open(DATADIR + 'hvd_dataservices_quality.json', 'w') as f:
        json.dump(hist_ds, f, indent=4)

    minio_open.send_files(
        list_files=[
            {
                "source_path": DATADIR,
                "source_name": 'hvd_dataservices_quality.json',
                "dest_path": minio_destination_folder,
                "dest_name": 'hvd_dataservices_quality.json',
            }
        ],
        ignore_airflow_env=True,
    )
