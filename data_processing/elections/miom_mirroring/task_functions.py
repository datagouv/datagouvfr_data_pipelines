from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.download import download_files

import os
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup

minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

ID_CURRENT_ELECTION = "LG2024"
URL_ELECTIONS_HTTP_SERVER = "https://www.resultats-elections.interieur.gouv.fr/telechargements/"

def parse_http_server(url_source, url, arr, max_date):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        tds = soup.find_all('td')
        i = 0
        mydict = {}
        root_folder = False
        for td in tds:
            if (i == 4):
                i = 0
                mydict = {}
                root_folder = False
            i += 1
            if (i == 1):
                soup2 = BeautifulSoup(str(td), 'html.parser')
                links = soup2.find_all('a')
                for link in links:
                    href = link.get('href')
                    if href != '../':
                        if not href.endswith('/'):
                            mydict["link"] = url + str(href)
                            mydict["name"] = str(href)
                        else:
                            arr, max_date = parse_http_server(url_source, url + href, arr, max_date)
                    else:
                        root_folder = True
            if (i == 2 and not root_folder):
                new_date = datetime.strptime(td.text, '%Y-%b-%d %H:%M:%S').isoformat()
                if new_date > max_date:
                    max_date = new_date
                    mydict["date"] = new_date
                    if 'link' in mydict:
                        arr.append(mydict)
    return arr, max_date


def get_files_updated_miom(ti):
    url = URL_ELECTIONS_HTTP_SERVER + ID_CURRENT_ELECTION + "/"
    r = requests.get(
        "https://object.data.gouv.fr/" + \
        MINIO_BUCKET_DATA_PIPELINE_OPEN + \
        "/" + AIRFLOW_ENV + \
        "/elections-mirroring/max_date.json"
    )
    max_date = r.json()["max_date"]
    arr = []
    arr, max_date = parse_http_server(url, url, arr, max_date)
    with open(f"{AIRFLOW_DAG_TMP}elections-mirroring/max_date.json", "w") as fp:
        json.dump({ "max_date": max_date }, fp)
    ti.xcom_push(key="miom_files", value=arr)
    ti.xcom_push(key="max_date", value=max_date)


def download_local_files(ti):
    miom_files = ti.xcom_pull(key="miom_files", task_ids="get_files_updated_miom")
    arr = []
    for cf in miom_files:
        arr.append(
            {
                "url": cf["link"],
                "dest_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/" + "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/"))[:-1] + "/",
                "dest_name": cf["name"]
            }
        )
    download_files(arr)

   
def send_to_minio(ti):
    miom_files = ti.xcom_pull(key="miom_files", task_ids="get_files_updated_miom")
    arr = []
    for cf in miom_files:
        arr.append(
            {
                "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/" + "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/"))[:-1] + "/",
                "source_name": cf["name"],
                "dest_path": "elections-mirroring/" + "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/"))[:-1] + "/",
                "dest_name": cf["name"],
            }
        )

    minio_open.send_files(
        list_files=arr
    )

    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/",
                "source_name": "max_date.json",
                "dest_path": "elections-mirroring/",
                "dest_name": "max_date.json",
            }
        ]
    )

    
def download_from_minio(ti):
    prefix = "elections-mirroring/" + ID_CURRENT_ELECTION + "/"
    minio_files = minio_open.get_files_from_prefix(
        prefix=prefix,
        ignore_airflow_env=False,
        recursive=True,
    )
    print(minio_files)
    os.makedirs(f"{AIRFLOW_DAG_TMP}elections-mirroring/export", exist_ok=True)
    list_files = []
    for mf in minio_files:
        list_files.append(
            {
                "source_path": "/".join(mf.split("/")[:-1]) + "/",
                "source_name": mf.split("/")[-1],
                "dest_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/export/" + "/".join(mf.split(prefix)[1].split("/")[:-1]) + "/",
                "dest_name": mf.split("/")[-1],
            }
        )
    
    minio_open.download_files(list_files=list_files)


def send_export_to_minio():
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/",
                "source_name": ID_CURRENT_ELECTION + ".zip",
                "dest_path": "elections-mirroring/",
                "dest_name": ID_CURRENT_ELECTION + ".zip",
            }
        ]
    )