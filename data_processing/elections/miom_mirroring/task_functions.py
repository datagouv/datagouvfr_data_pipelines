import os
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import glob
import time

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_HOME,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
)

minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}elections-mirroring/"
ID_CURRENT_ELECTION = "LG2024"
URL_ELECTIONS_HTTP_SERVER = "https://www.resultats-elections.interieur.gouv.fr/telechargements/"


def parse_http_server(url, max_dates, new_max_dates, arr, filename):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        tds = soup.find_all('td')
        i = 0
        mydict = {}
        found = False
        for td in tds:
            if (i == 4):
                i = 0
                mydict = {}
            i += 1
            if (i == 1):
                soup2 = BeautifulSoup(str(td), 'html.parser')
                links = soup2.find_all('a')
                for link in links:
                    href = link.get('href')
                    for complement in [".xml", "COM.xml", "CIR.xml"]:
                        if str(href) == filename + complement:
                            mydict["link"] = url + str(href)
                            mydict["name"] = str(href)
                            found = True
            if (i == 2 and found):
                new_date = datetime.strptime(td.text, '%Y-%b-%d %H:%M:%S').isoformat()
                found = False
                if filename not in max_dates or (filename in max_dates and max_dates[filename] < new_date):
                    mydict["date"] = new_date
                    arr.append(mydict)
                    new_max_dates[mydict["name"]] = new_date
    return arr, new_max_dates


def get_dpt_list():
    # department_list = ["01", "02"]
    department_list = [f'{i:02}' for i in range(1, 96) if i != 20]
    department_list += [str(i) for i in range(971, 979)]
    additional_departments = ['2A', '2B', 'ZZ', 'ZX', '986', '987', '988']
    department_list += additional_departments
    return department_list


def get_files_updated_miom(ti):
    url = URL_ELECTIONS_HTTP_SERVER + ID_CURRENT_ELECTION + "/"
    url_max_date = (
        f"https://object.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/" +
        f"{AIRFLOW_ENV}/elections-mirroring/{ID_CURRENT_ELECTION}/max_date.json"
    )
    r = requests.get(url_max_date)
    max_dates = r.json()
    new_max_dates = {}
    arr = []

    for dep in get_dpt_list():
        for tour in ["1", "2"]:
            for file in ["candidatsT", "resultatsT"]:
                print(url)
                url = URL_ELECTIONS_HTTP_SERVER + ID_CURRENT_ELECTION + "/" + file + tour + "/" + dep + "/"
                arr, new_max_dates = parse_http_server(
                    url,
                    max_dates,
                    new_max_dates,
                    arr,
                    file[0].upper() + tour + dep
                )

    with open(f"{AIRFLOW_DAG_TMP}elections-mirroring/max_date.json", "w") as fp:
        json.dump(new_max_dates, fp)

    ti.xcom_push(key="miom_files", value=arr)
    ti.xcom_push(key="max_date", value=new_max_dates)


def download_local_files(ti):
    miom_files = ti.xcom_pull(key="miom_files", task_ids="get_files_updated_miom")
    for cf in miom_files:
        url = cf["link"]
        dest_path = (
            f"{AIRFLOW_DAG_TMP}elections-mirroring/" +
            "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/")[:-1]) + "/"
        )
        dest_name = cf["name"]

        if (
            "resultatsT" in url and ('COM.xml' in dest_name or 'CIR.xml' in dest_name)
        ) or ("resultatsT" not in url):
            attempt = 0
            isNotDownload = True
            os.makedirs(dest_path, exist_ok=True)
            while isNotDownload:
                attempt += 1
                if attempt == 3:
                    isNotDownload = False
                try:
                    with requests.get(url, stream=True) as r:
                        r.raise_for_status()
                        with open(f"{dest_path}{dest_name}", "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    isNotDownload = False
                    print(f"Successfully downloaded {dest_name} on attempt {attempt}")
                except requests.RequestException as e:
                    print(f"Attempt {attempt} failed: {e}")
                    time.sleep(1)


def send_to_minio(ti):
    miom_files = ti.xcom_pull(key="miom_files", task_ids="get_files_updated_miom")
    minio_open.send_files(
        list_files=[
            File(
                source_path=(
                    f"{AIRFLOW_DAG_TMP}elections-mirroring/" +
                    "/".join(cf["link"].replace(URL_ELECTIONS_HTTP_SERVER, "").split("/")[:-1]) + "/"
                ),
                source_name=cf["name"],
                dest_path=(
                    f"elections-mirroring/{ID_CURRENT_ELECTION}/data/" +
                    "/".join(
                        cf["link"].replace(URL_ELECTIONS_HTTP_SERVER + ID_CURRENT_ELECTION + "/", "")
                        .split("/")[:-1]
                    ) + "/"
                ),
                dest_name=cf["name"],
            ) for cf in miom_files
        ]
    )

    minio_open.send_files(
        list_files=[
            File(
                source_path=f"{AIRFLOW_DAG_TMP}elections-mirroring/",
                source_name="max_date.json",
                dest_path="elections-mirroring/" + ID_CURRENT_ELECTION + "/",
                dest_name="max_date.json",
            )
        ]
    )


def download_from_minio():
    prefix = "elections-mirroring/" + ID_CURRENT_ELECTION + "/data/"
    minio_files = minio_open.get_files_from_prefix(
        prefix=prefix,
        ignore_airflow_env=False,
        recursive=True,
    )
    print(minio_files)
    os.makedirs(f"{AIRFLOW_DAG_TMP}elections-mirroring/export", exist_ok=True)
    minio_open.download_files(
        list_files=[
            File(
                source_path="/".join(mf.split("/")[:-1]) + "/",
                source_name=mf.split("/")[-1],
                dest_path=(
                    f"{AIRFLOW_DAG_TMP}elections-mirroring/export/" +
                    "/".join(mf.split(prefix)[1].split("/")[:-1]) + "/"
                ),
                dest_name=mf.split("/")[-1],
                remote_source=True,
            ) for mf in minio_files
        ]
    )


def send_exports_to_minio():
    list_files = [
        File(
            source_path=f"{AIRFLOW_DAG_TMP}elections-mirroring/",
            source_name=ID_CURRENT_ELECTION + ".zip",
            dest_path="elections-mirroring/" + ID_CURRENT_ELECTION + "/",
            dest_name=ID_CURRENT_ELECTION + ".zip",
        )
    ]
    for typeCandidat in ['candidatsT1', 'candidatsT2']:
        if os.path.exists(f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeCandidat}.csv"):
            list_files.append(
                File(
                    source_path=f"{AIRFLOW_DAG_TMP}elections-mirroring/",
                    source_name=f"{typeCandidat}.csv",
                    dest_path="elections-mirroring/" + ID_CURRENT_ELECTION + "/",
                    dest_name=f"{typeCandidat}.csv",
                )
            )

    for typeResultat in ['resultatsT1', 'resultatsT2']:
        for levelResultat in ['communes', 'circos']:
            for typeResultatFile in ['general', 'candidats']:
                if os.path.exists(
                    f"{AIRFLOW_DAG_TMP}elections-mirroring/"
                    f"{typeResultat}_{levelResultat}_{typeResultatFile}.csv"
                ):
                    list_files.append(
                        File(
                            source_path=f"{AIRFLOW_DAG_TMP}elections-mirroring/",
                            source_name=f"{typeResultat}_{levelResultat}_{typeResultatFile}.csv",
                            dest_path="elections-mirroring/" + ID_CURRENT_ELECTION + "/",
                            dest_name=f"{typeResultat}_{levelResultat}_{typeResultatFile}.csv",
                        )
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
    max_dates = ti.xcom_pull(key="max_date", task_ids="get_files_updated_miom")
    max_date = "1970-01-01"
    for md in max_dates:
        if max_dates[md] > max_date:
            max_date = max_dates[md]
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
            dataset_id=d[AIRFLOW_ENV]["dataset_id"],
            resource_id=d[AIRFLOW_ENV]["resource_id"],
            payload={
                "url": d['url'],
                "filesize": filesize,
                "title": d['name'] + complement,
                "format": d['format'],
                "description": "",
            },
            on_demo=True,
        )


def manage_data(item, level, typefile, mydict_general, mydict_com, candidat):
    mydict = {}
    mydict = mydict_general.copy()
    for tour in item.find_all('Tour'):
        if level == "communes":
            mydict["CodCom"] = mydict_com["CodCom"]
            mydict["LibCom"] = mydict_com["LibCom"]

        if typefile == "general":
            mydict["NumTour"] = tour.find('NumTour').text
            mydict["Inscrits"] = tour.find('Inscrits').find('Nombre').text
            mydict["Abstentions"] = tour.find('Abstentions').find('Nombre').text
            mydict["Abstentions_RapportInscrit"] = tour.find('Abstentions').find('RapportInscrits').text
            mydict["Votants"] = tour.find('Votants').find('Nombre').text
            mydict["Votants_RapportInscrit"] = tour.find('Votants').find('RapportInscrits').text
            mydict["Blancs"] = tour.find('Blancs').find('Nombre').text
            mydict["Blancs_RapportInscrit"] = tour.find('Blancs').find('RapportInscrits').text
            mydict["Blancs_RapportVotant"] = tour.find('Blancs').find('RapportVotants').text
            mydict["Nuls"] = tour.find('Nuls').find('Nombre').text
            mydict["Nuls_RapportInscrit"] = tour.find('Nuls').find('RapportInscrits').text
            mydict["Nuls_RapportVotant"] = tour.find('Nuls').find('RapportVotants').text
            mydict["Exprimes"] = tour.find('Exprimes').find('Nombre').text
            mydict["Exprimes_RapportInscrit"] = tour.find('Exprimes').find('RapportInscrits').text
            mydict["Exprimes_RapportVotant"] = tour.find('Exprimes').find('RapportVotants').text

        if typefile == "candidats":
            mydict["NumTour"] = tour.find('NumTour').text
            mydict["NumPanneauCand"] = candidat.find('NumPanneauCand').text
            mydict["NomPsn"] = candidat.find('NomPsn').text
            mydict["PrenomPsn"] = candidat.find('PrenomPsn').text
            mydict["CivilitePsn"] = candidat.find('CivilitePsn').text
            mydict["CodNuaCand"] = candidat.find('CodNuaCand').text
            mydict["LibNuaCand"] = candidat.find('LibNuaCand').text
            mydict["NbVoix"] = candidat.find('NbVoix').text
            mydict["RapportExprimes"] = candidat.find('RapportExprimes').text
            mydict["RapportInscrits"] = candidat.find('RapportInscrits').text

    return mydict


def manage_cases(xml_data, level):
    soup = BeautifulSoup(xml_data, 'xml')
    general_results = []
    candidats_results = []

    base_dict = {
        "Type": soup.find('Type').text,
        "Annee": soup.find('Annee').text
    }

    for department in soup.find_all('Departement'):
        department_dict = base_dict.copy()
        department_dict["CodDpt"] = department.find('CodDpt').text
        department_dict["LibDpt"] = department.find('LibDpt').text

        for circonscription in department.find_all('Circonscription'):
            circonscription_dict = department_dict.copy()
            circonscription_dict["CodCirElec"] = circonscription.find('CodCirElec').text
            circonscription_dict["LibCirElec"] = circonscription.find('LibCirElec').text

            if level == "communes":
                for commune in circonscription.find_all('Commune'):
                    mydict_com = {
                        "CodCom": commune.find('CodCom').text,
                        "LibCom": commune.find('LibCom').text
                    }

                    general_result = manage_data(
                        commune, level,
                        "general",
                        circonscription_dict,
                        mydict_com,
                        None
                    )
                    general_results.append(general_result)

                    for candidat in commune.find_all('Candidat'):
                        candidat_dict = circonscription_dict.copy()
                        candidats_result = manage_data(
                            commune,
                            level,
                            "candidats",
                            candidat_dict,
                            mydict_com,
                            candidat
                        )
                        candidats_results.append(candidats_result)

            else:
                general_result = manage_data(
                    circonscription,
                    level,
                    "general",
                    circonscription_dict,
                    None,
                    None
                )
                general_results.append(general_result)

                for candidat in circonscription.find_all('Candidat'):
                    candidat_dict = circonscription_dict.copy()
                    candidats_result = manage_data(
                        circonscription,
                        level,
                        "candidats",
                        candidat_dict,
                        None,
                        candidat
                    )
                    candidats_results.append(candidats_result)

    return pd.DataFrame(general_results), pd.DataFrame(candidats_results)


def process_xml_resultats(xml_data, level):
    dfinter1, dfinter2 = manage_cases(xml_data, level)
    return dfinter1, dfinter2


def create_resultats_files():
    files = glob.glob(f"{AIRFLOW_DAG_TMP}elections-mirroring/export/**", recursive=True)

    typeFiles = [
        {
            "level": "communes",
            "code": "COM.xml"
        },
        {
            "level": "circos",
            "code": "CIR.xml"
        }
    ]
    for tf in typeFiles:
        print("-----", tf["level"])
        for typeResultat in ['resultatsT1', 'resultatsT2']:
            print("--------", typeResultat)
            df1 = pd.DataFrame()
            df2 = pd.DataFrame()
            for f in files:
                if typeResultat in f and (tf["code"] in f):
                    print(f)
                    with open(f, 'r', encoding='utf-8') as file:
                        xml_data = file.read()
                    dfinter1, dfinter2 = process_xml_resultats(xml_data, tf["level"])
                    df1 = pd.concat([df1, dfinter1])
                    df2 = pd.concat([df2, dfinter2])
            if df1.shape[0] > 0:
                df1.to_csv(
                    f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_{tf['level']}_general.csv",
                    index=False
                )
            else:
                data = {'Données': ['Non disponibles']}
                df1 = pd.DataFrame(data)
                df1.to_csv(
                    f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_{tf['level']}_general.csv",
                    index=False
                )

            if df2.shape[0] > 0:
                df2.to_csv(
                    f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_{tf['level']}_candidats.csv",
                    index=False
                )
            else:
                data = {'Données': ['Non disponibles']}
                df2 = pd.DataFrame(data)
                df2.to_csv(
                    f"{AIRFLOW_DAG_TMP}elections-mirroring/{typeResultat}_{tf['level']}_candidats.csv",
                    index=False
                )
