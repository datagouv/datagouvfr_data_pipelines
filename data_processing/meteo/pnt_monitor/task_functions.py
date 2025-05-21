import requests
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import json
import urllib3
from minio import datatypes

from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.config import (
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MINIO_BUCKET_PNT,
    SECRET_MINIO_PNT_USER,
    SECRET_MINIO_PNT_PASSWORD,
    AIRFLOW_ENV,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import local_client

minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
minio_pnt = MinIOClient(
    bucket=MINIO_BUCKET_PNT,
    user=SECRET_MINIO_PNT_USER,
    pwd=SECRET_MINIO_PNT_PASSWORD,
    http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=30, read=1800)),
)
too_old_filename = "too_old.json"


def get_timeslot_and_paquet(url):
    """
    URLs look like this:
    https://object.data.gouv.fr/meteofrance-pnt/pnt/2024-03-13T00:00:00Z/
    arome-om/ANTIL/0025/HP1/arome-om-ANTIL__0025__HP1__001H__2024-03-13T00:00:00Z.grib2
    So we get ('2024-03-13T00:00:00Z', 'arome-om')
    """
    _ = url.split('/')
    return _[5], _[6]


def threshold_in_the_past(nb_batches_behind=3):
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
        "https://www.data.gouv.fr/api/1/topics/65e0c82c2da27c1dff5fa66f/",
        headers={'X-fields': 'datasets{id,title}'},
    ).json()['datasets']

    time_slots = {}
    unavailable_resources = {}
    too_old = defaultdict(list)
    for d in pnt_datasets:
        print(d)
        resources = requests.get(
            f"https://www.data.gouv.fr/api/1/datasets/{d["id"]}/",
            headers={'X-fields': 'resources{id,url,title}'},
        ).json()['resources']
        for r in resources:
            if "MFWAM/05" in r['url']:
                # MFWAM 0.5 is deprecated since April 2025
                continue
            if 'object.data.gouv.fr' in r['url']:
                ts, pq = get_timeslot_and_paquet(r['url'])
                if ts < threshold:
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
    print("Unavailable resources:", unavailable_resources)
    print("Too old resources:", too_old)
    nb_too_old = sum([len(too_old[d]) for d in too_old])
    print(nb_too_old, "resources are too old")

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
        # if alert:
        #     message += "@geoffrey.aldebert "
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
            File(
                source_path=AIRFLOW_DAG_TMP,
                source_name=too_old_filename,
                dest_path="pnt/",
                dest_name=too_old_filename,
            )
        ]
    )

    if message:
        message = "##### :crystal_ball: :sun_behind_rain_cloud: Données PNT\n" + message
        if len(message) > 55000:
            message = message[:55000] + "\n\nEt plus encore !"
        send_message(message)


def update_tree():
    # listing current runs on minio
    current_runs = sorted([pref for pref in minio_pnt.get_files_from_prefix(
        prefix="pnt/",
        ignore_airflow_env=True,
        recursive=False,
    )])
    # getting current tree
    tree = requests.get('https://www.data.gouv.fr/fr/datasets/r/ab77c9d0-3db4-4c2f-ae56-5a52ae824eeb').json()
    # removing runs that have been deleted since last DAG run
    to_delete = [k for k in tree['pnt'] if k not in [r.split('/')[1] for r in current_runs]]
    for run in to_delete:
        print(f"> Deleting {run}")
        del tree['pnt'][run]
    # getting tree for each new run
    to_add = [r.split('/')[1] for r in current_runs if r.split('/')[1] not in tree['pnt']]
    for run in to_add:
        print(f"> Adding {run}")
        run_tree = build_tree(minio_pnt.get_files_from_prefix(
            prefix=f"pnt/{run}/",
            ignore_airflow_env=True,
            recursive=True,
        ))
        tree['pnt'][run] = run_tree['pnt'][run]
    # just to make sure
    assert len(tree["pnt"]) == len(current_runs)
    return tree


def build_tree(paths: list):
    tree = {}
    for _, path in enumerate(paths):
        if isinstance(path, datatypes.Object):
            path = path.object_name
        parts = path.split('/')
        *dirs, file = parts
        current_level = tree
        for idx, part in enumerate(dirs):
            if idx == len(dirs) - 1:
                if not current_level.get(part):
                    current_level[part] = []
                current_level[part].append(file)
            elif part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]
    return tree


def dump_and_send_tree() -> None:
    tree = update_tree()
    # runs look like this 2024-10-09T18:00:00Z
    oldest = sorted([run[:-1] for run in tree["pnt"]])[0]
    with open('./pnt_tree.json', 'w') as f:
        json.dump(tree, f)

    local_client.resource(
        id="ab77c9d0-3db4-4c2f-ae56-5a52ae824eeb",
        dataset_id="66d02b7174375550d7b10f3f",
        fetch=False,
        _from_response={"filetype": "file"},  # to be able to update the file without fetching
    ).update(
        file_to_upload="./pnt_tree.json",
        payload={"title": "Arborescence des dossiers sur le dépôt"}
    )
    local_client.dataset("66d02b7174375550d7b10f3f", fetch=False).update(
        payload={
            "temporal_coverage": {
                "start": oldest + ".000000+00:00",
                "end": datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            },
            "tags": ["hvd", "meteorologiques"],
        },
    )


def consolidate_logs():
    logs, csvs = [], []
    for o in minio_pnt.get_files_from_prefix(
        prefix="logs/",
        ignore_airflow_env=True,
        recursive=False,
    ):
        if o.endswith(".log"):
            logs.append(o)
        elif o.endswith(".csv"):
            csvs.append(o.replace("logs/", "").replace(".csv", ""))
        else:
            raise ValueError(f"Unexpected file here: {o}")
    print(len(logs), "new logs")
    print(len(csvs), "existing csv files")
    dates = defaultdict(list)
    for log_file in logs:
        content = minio_pnt.get_file_content(log_file)
        date = content.split(";")[1].split(" ")[0]
        dates[date].append(content + "\n")
    for date in dates:
        print(f"> Processing {date}")
        existing = None
        if date in csvs:
            existing = minio_pnt.get_file_content(f"logs/{date}.csv")
            with open(AIRFLOW_DAG_TMP + f"{date}.csv", "w") as f:
                f.write(existing)
            print("Updating existing file")
        with open(AIRFLOW_DAG_TMP + f"{date}.csv", "a" if existing else "w") as f:
            if not existing:
                print("Creating new file")
                f.write("file;error_datetime\n")
            for row in dates[date]:
                f.write(row)
        minio_pnt.send_files(
            list_files=[
                File(
                    source_path=AIRFLOW_DAG_TMP,
                    source_name=f"{date}.csv",
                    dest_path="logs/",
                    dest_name=f"{date}.csv",
                ),
            ],
            ignore_airflow_env=True,
            burn_after_sending=True,
        )
    for log_file in logs:
        minio_pnt.delete_file(log_file)
