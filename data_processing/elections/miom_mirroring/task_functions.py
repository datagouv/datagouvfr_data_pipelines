from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_HOME,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
)

import os
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import glob

minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}elections-mirroring/"
ID_CURRENT_ELECTION = "LG2024"
URL_ELECTIONS_HTTP_SERVER = "https://www.resultats-elections.interieur.gouv.fr/telechargements/"


def parse_http_server(url, arr, max_date, item_max_date, subfolder):
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
                            arr, item_max_date = parse_http_server(url + str(href), arr, max_date, item_max_date, subfolder)
                    else:
                        root_folder = True
            if (i == 2 and not root_folder):
                new_date = datetime.strptime(td.text, '%Y-%b-%d %H:%M:%S').isoformat()
                if new_date > max_date:
                    mydict["date"] = new_date
                    if 'link' in mydict:
                        if 'resultatsT' not in subfolder or ('com.xml' in mydict['link'] and 'resultatsT' in subfolder):
                            arr.append(mydict)
                if new_date > item_max_date:
                    item_max_date = new_date
    return arr, item_max_date


def parse_and_max_date(url, arr, max_date, item, new_max_date):
    arr, item_max_date = parse_http_server(f"{url}{item}/", arr, max_date, max_date, "{item}")
    if new_max_date < item_max_date:
        new_max_date = item_max_date
    return arr, new_max_date


def get_files_updated_miom(ti):
    url = URL_ELECTIONS_HTTP_SERVER + ID_CURRENT_ELECTION + "/"
    r = requests.get(
        "https://object.data.gouv.fr/" + \
        MINIO_BUCKET_DATA_PIPELINE_OPEN + \
        "/" + AIRFLOW_ENV + \
        "/elections-mirroring/" + ID_CURRENT_ELECTION + "/max_date.json"
    )
    max_date = r.json()["max_date"]
    new_max_date = max_date
    arr = []
    for item in ['nuances', 'candidatsT1', 'candidatsT2', 'resultatsT1', 'resultatsT2']:
        arr, new_max_date = parse_and_max_date(url, arr, max_date, item, new_max_date)

    with open(f"{AIRFLOW_DAG_TMP}elections-mirroring/max_date.json", "w") as fp:
        json.dump({ "max_date": new_max_date }, fp)
    ti.xcom_push(key="miom_files", value=arr)
    ti.xcom_push(key="max_date", value=new_max_date)


def download_local_files(ti):
    miom_files = ti.xcom_pull(key="miom_files", task_ids="get_files_updated_miom")
    arr = []
    for cf in miom_files:
        arr.append(
            {
                "url": cf["link"],
                "dest_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/" + "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/")[:-1]) + "/",
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
                "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/" + '/' + "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/")[:-1]) + "/",
                "source_name": cf["name"],
                "dest_path": "elections-mirroring/" + "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/")[:-1]) + "/data/",
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
                "dest_path": "elections-mirroring/" + ID_CURRENT_ELECTION + "/",
                "dest_name": "max_date.json",
            }
        ]
    )

    
def download_from_minio(ti):
    prefix = "elections-mirroring/" + ID_CURRENT_ELECTION + "/data/" 
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


def send_exports_to_minio():
    list_files = [
        {
            "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/",
            "source_name": ID_CURRENT_ELECTION + ".zip",
            "dest_path": "elections-mirroring/" + ID_CURRENT_ELECTION + "/",
            "dest_name": ID_CURRENT_ELECTION + ".zip",
        }
    ]
    for typeCandidat in ['candidatsT1', 'candidatsT2']:
        if os.path.exists(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeCandidat}.csv"):
            list_files.append(
                {
                    "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/",
                    "source_name": f"{typeCandidat}.csv",
                    "dest_path": "elections-mirroring/" + ID_CURRENT_ELECTION + "/",
                    "dest_name": f"{typeCandidat}.csv",
                }
            )

    for typeResultat in ['resultatsT1', 'resultatsT2']:
        for typeResultatFile in ['general', 'candidats']:
            if os.path.exists(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_{typeResultatFile}.csv"):
                list_files.append(
                    {
                        "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/",
                        "source_name": f"{typeResultat}_{typeResultatFile}.csv",
                        "dest_path": "elections-mirroring/" + ID_CURRENT_ELECTION + "/",
                        "dest_name": f"{typeResultat}_{typeResultatFile}.csv",
                    }
                )
    minio_open.send_files(
        list_files=list_files
    )


def check_if_continue(ti):
    miom_files = ti.xcom_pull(key="miom_files", task_ids="get_files_updated_miom")
    if len(miom_files) == 0:
        return False
    return True


def process_xml_candidats(xml_data):
    df = pd.DataFrame()
    soup = BeautifulSoup(xml_data, 'xml')

    if not soup.find('Circonscriptions'):
        election_type = soup.find('Type').text
        election_year = soup.find('Annee').text
        department_code = soup.find('CodDpt').text
        department_name = soup.find('LibDpt').text
        circonscription_code = soup.find('CodCirElec').text
        circonscription_name = soup.find('LibCirElec').text

        candidates = []
        for candidat in soup.find_all('Candidat'):
            candidate_data = {
                'Type': election_type,
                'Annee': election_year,
                'CodDpt': department_code,
                'LibDpt': department_name,
                'CodCirElec': circonscription_code,
                'LibCirElec': circonscription_name,
                'NumPanneauCand': candidat.find('NumPanneauCand').text,
                'NomPsn': candidat.find('NomPsn').text,
                'PrenomPsn': candidat.find('PrenomPsn').text,
                'CivilitePsn': candidat.find('CivilitePsn').text,
                'CodNuaCand': candidat.find('CodNuaCand').text,
                'LibNuaCand': candidat.find('LibNuaCand').text,
            }
            candidates.append(candidate_data)

        df = pd.DataFrame(candidates)
    return df


def create_candidats_files():
    files = glob.glob(f"{AIRFLOW_DAG_TMP}elections-mirroring/export/**", recursive=True)
    for typeCandidat in ['candidatsT1', 'candidatsT2']:
        df = pd.DataFrame()
        for f in files:
            if typeCandidat in f and '.xml' in f:
                print(f)
                with open(f, 'r', encoding='utf-8') as file:
                    xml_data = file.read()
                dfinter = process_xml_candidats(xml_data)
                df = pd.concat([df, dfinter])
        if df.shape[0] > 0:
            df.to_csv(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeCandidat}.csv", index=False)
        else:
            data = {'Données': ['Non disponibles']}
            df = pd.DataFrame(data)
            df.to_csv(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeCandidat}.csv", index=False)


def publish_results_elections(ti):
    max_date = ti.xcom_pull(key="max_date", task_ids="get_files_updated_miom")
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/miom_mirroring/config/dgv.json") as fp:
        data = json.load(fp)
    for d in data:
        complement = ' (dernière mise à jour : ' + max_date + ')'
        if d['format'] == 'csv':
            df = pd.read_csv(DATADIR + d['filename'])
            if df.shape[0] <= 1:
                complement = ' (pas encore disponible)'

        filesize = None
        if d["filename"]:
            filesize = os.path.getsize(os.path.join(DATADIR, d["filename"]))
            
        post_remote_resource(
            remote_url=d['url'],
            dataset_id=d[AIRFLOW_ENV]["dataset_id"],
            resource_id=d[AIRFLOW_ENV]["resource_id"],
            filesize=filesize,
            title=d['name'] + complement,
            format=d['format'],
            description="",
            datagouv_url="https://demo.data.gouv.fr",
        )


def process_xml_general(xml_data):
    soup = BeautifulSoup(xml_data, 'xml')
    general_results = []

    election_type = soup.find('Type').text
    election_year = soup.find('Annee').text

    for department in soup.find_all('Departement'):
        department_code = department.find('CodDpt').text
        department_name = department.find('LibDpt').text

        for commune in department.find_all('Commune'):
            commune_code = commune.find('CodSubCom').text
            commune_name = commune.find('LibSubCom').text
            circonscription_code = commune.find('CodCirLg').text
            circonscription_name = commune.find('LibFraSubCom').text

            for tour in commune.find_all('Tour'):
                tour_number = tour.find('NumTour').text
                inscrits = tour.find('Inscrits').find('Nombre').text
                abstentions = tour.find('Abstentions').find('Nombre').text
                abstentions_ratio = tour.find('Abstentions').find('RapportInscrit').text
                votants = tour.find('Votants').find('Nombre').text
                votants_ratio = tour.find('Votants').find('RapportInscrit').text
                blancs = tour.find('Blancs').find('Nombre').text
                blancs_ratio_inscrit = tour.find('Blancs').find('RapportInscrit').text
                blancs_ratio_votant = tour.find('Blancs').find('RapportVotant').text
                nuls = tour.find('Nuls').find('Nombre').text
                nuls_ratio_inscrit = tour.find('Nuls').find('RapportInscrit').text
                nuls_ratio_votant = tour.find('Nuls').find('RapportVotant').text
                exprimes = tour.find('Exprimes').find('Nombre').text
                exprimes_ratio_inscrit = tour.find('Exprimes').find('RapportInscrit').text
                exprimes_ratio_votant = tour.find('Exprimes').find('RapportVotant').text

                general_results.append({
                    'Type': election_type,
                    'Annee': election_year,
                    'CodDpt': department_code,
                    'LibDpt': department_name,
                    'CodSubCom': commune_code,
                    'LibSubCom': commune_name,
                    'CodCirLg': circonscription_code,
                    'LibFraSubCom': circonscription_name,
                    'NumTour': tour_number,
                    'Inscrits': inscrits,
                    'Abstentions': abstentions,
                    'Abstentions_RapportInscrit': abstentions_ratio,
                    'Votants': votants,
                    'Votants_RapportInscrit': votants_ratio,
                    'Blancs': blancs,
                    'Blancs_RapportInscrit': blancs_ratio_inscrit,
                    'Blancs_RapportVotant': blancs_ratio_votant,
                    'Nuls': nuls,
                    'Nuls_RapportInscrit': nuls_ratio_inscrit,
                    'Nuls_RapportVotant': nuls_ratio_votant,
                    'Exprimes': exprimes,
                    'Exprimes_RapportInscrit': exprimes_ratio_inscrit,
                    'Exprimes_RapportVotant': exprimes_ratio_votant
                })

    return pd.DataFrame(general_results)


def process_xml_candidats(xml_data):
    soup = BeautifulSoup(xml_data, 'xml')
    candidate_results = []

    election_type = soup.find('Type').text
    election_year = soup.find('Annee').text

    for department in soup.find_all('Departement'):
        department_code = department.find('CodDpt').text
        department_name = department.find('LibDpt').text

        for commune in department.find_all('Commune'):
            commune_code = commune.find('CodSubCom').text
            commune_name = commune.find('LibSubCom').text
            circonscription_code = commune.find('CodCirLg').text
            circonscription_name = commune.find('LibFraSubCom').text

            for tour in commune.find_all('Tour'):
                tour_number = tour.find('NumTour').text

                for candidat in tour.find_all('Candidat'):
                    candidate_results.append({
                        'Type': election_type,
                        'Annee': election_year,
                        'CodDpt': department_code,
                        'LibDpt': department_name,
                        'CodSubCom': commune_code,
                        'LibSubCom': commune_name,
                        'CodCirLg': circonscription_code,
                        'LibFraSubCom': circonscription_name,
                        'NumTour': tour_number,
                        'NumPanneauCand': candidat.find('NumPanneauCand').text,
                        'NomPsn': candidat.find('NomPsn').text,
                        'PrenomPsn': candidat.find('PrenomPsn').text,
                        'CivilitePsn': candidat.find('CivilitePsn').text,
                        'CodNua': candidat.find('CodNua').text,
                        'LibNua': candidat.find('LibNua').text,
                        'NbVoix': candidat.find('NbVoix').text,
                        'RapportExprime': candidat.find('RapportExprime').text,
                        'RapportInscrit': candidat.find('RapportInscrit').text
                    })

    return pd.DataFrame(candidate_results)


def create_resultats_files():
    files = glob.glob(f"{AIRFLOW_DAG_TMP}elections-mirroring/export/**", recursive=True)
    for typeResultat in ['resultatsT1', 'resultatsT2']:
        df1 = pd.DataFrame()
        df2 = pd.DataFrame()
        for f in files:
            if typeResultat in f and 'com.xml' in f:
                print(f)
                with open(f, 'r', encoding='utf-8') as file:
                    xml_data = file.read()
                dfinter1 = process_xml_general(xml_data)
                dfinter2 = process_xml_candidats(xml_data)
                df1 = pd.concat([df1, dfinter1])
                df2 = pd.concat([df2, dfinter2])
        if df1.shape[0] > 0:
            df1.to_csv(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_general.csv", index=False)
        else:
            data = {'Données': ['Non disponibles']}
            df1 = pd.DataFrame(data)
            df1.to_csv(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_general.csv", index=False)
        
        if df2.shape[0] > 0:
            df2.to_csv(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_candidats.csv", index=False)
        else:
            data = {'Données': ['Non disponibles']}
            df2 = pd.DataFrame(data)
            df2.to_csv(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_candidats.csv", index=False)
