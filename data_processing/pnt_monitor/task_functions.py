import requests
from datetime import datetime
from datetime import timedelta
import json
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.config import (
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    AIRFLOW_ENV,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message

api_url = "https://www.data.gouv.fr/api/1/"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
too_old_filename = "too_old.json"


def get_timeslot_and_paquet(url):
    """
    URLs look like this:
    https://object.data.gouv.fr/meteofrance-pnt/pnt/2024-03-13T00:00:00Z/
    arome-om/ANTIL/0025/HP1/arome-om-ANTIL__0025__HP1__001H__2024-03-13T00:00:00Z.grib2
    """
    _ = url.split('/')
    return _[5], _[6]


def threshold_in_the_past(nb_batches_behind=2):
    now = datetime.now()
    if now.hour < 6:
        batch_hour = 0
    elif now.hour < 12:
        batch_hour = 6
    elif now.hour < 18:
        batch_hour = 12
    elif now.hour >= 18:
        batch_hour = 18
    batch_time = now.replace(second=0, microsecond=0, minute=0, hour=batch_hour)
    return (batch_time - timedelta(hours=6 * nb_batches_behind)).strftime("%Y-%m-%dT%H:%M:%SZ")


def scan_pnt_files(ti):
    threshold = threshold_in_the_past()
    pnt_datasets = requests.get(
        api_url + 'topics/65e0c82c2da27c1dff5fa66f/',
        headers={'X-fields': 'datasets{id,title}'},
    ).json()['datasets']

    time_slots = {}
    unavailable_resources = {}
    too_old = {}
    for d in pnt_datasets:
        print(d)
        resources = requests.get(
            api_url + f'datasets/{d["id"]}/',
            headers={'X-fields': 'resources{id,url,title}'},
        ).json()['resources']
        for r in resources:
            if 'object.data.gouv.fr' in r['url']:
                ts, pq = get_timeslot_and_paquet(r['url'])
                if ts < threshold:
                    if d['title'] not in too_old:
                        too_old[d['title']] = []
                    too_old[d['title']].append([r['title'], d['id'], r['id']])
                if ts not in time_slots:
                    time_slots[ts] = {}
                if pq not in time_slots[ts]:
                    time_slots[ts][pq] = 0
                time_slots[ts][pq] += 1
            elif r['url'] == 'http://test.com':
                if d['title'] not in unavailable_resources:
                    unavailable_resources[d['title']] = []
                unavailable_resources[d['title']].append([r['title'], d['id'], r['id']])

    with open(AIRFLOW_DAG_TMP + too_old_filename, "w") as f:
        json.dump(too_old, f, ensure_ascii=False)

    ti.xcom_push(key="time_slots", value=time_slots)
    ti.xcom_push(key="unavailable_resources", value=unavailable_resources)
    ti.xcom_push(key="too_old", value=too_old)


def notification_mattermost(ti):
    unavailable_resources = ti.xcom_pull(key="unavailable_resources", task_ids="scan_pnt_files")
    too_old = ti.xcom_pull(key="too_old", task_ids="scan_pnt_files")
    print(unavailable_resources)
    print(too_old)
    nb_too_old = sum([len(too_old[d]) for d in too_old])
    print(nb_too_old)

    # saving the list of issues to ping only if new issues
    previous_too_old = json.loads(minio_open.get_file_content(
        f"{AIRFLOW_ENV}/pnt/too_old.json"
    ))
    prev = []
    for dataset in previous_too_old.values():
        for f, _, _ in dataset:
            prev.append(f)

    alert = False
    for dataset in too_old.values():
        for f, _, _ in dataset:
            alert = alert or f not in prev

    message = ""
    if too_old:
        message += ":warning: "
        if alert:
            message += "@geoffrey.aldebert "
        message += f"Ces {nb_too_old} ressources n'ont pas été mises à jour récemment :"
        for dataset in too_old:
            message += f'\n- {dataset}:'
            for k in too_old[dataset]:
                message += f'\n   - [{k[0]}](https://www.data.gouv.fr/fr/datasets/{k[1]}/#/resources/{k[2]})'

    if unavailable_resources:
        if message:
            message += '\n\n'
        message += "Certaines ressources n'ont pas de fichier associé :"
        for dataset in unavailable_resources:
            message += f'\n- {dataset}:'
            for k in unavailable_resources[dataset]:
                message += f'\n   - [{k[0]}](https://www.data.gouv.fr/fr/datasets/{k[1]}/#/resources/{k[2]})'

    minio_open.send_files(
        list_files=[
            {
                "source_path": AIRFLOW_DAG_TMP,
                "source_name": too_old_filename,
                "dest_path": "pnt/",
                "dest_name": too_old_filename,
            },
        ]
    )

    if message:
        message = "##### :crystal_ball: :sun_behind_rain_cloud: Données PNT\n" + message
        if len(message) > 60000:
            message = message[:60000] + "\n\nEt plus encore !"
        send_message(message)
