from airflow.operators.bash import BashOperator
from minio.error import S3Error

from datetime import datetime, timedelta
from unicodedata import normalize
import os
from minio import Minio
import glob
import unidecode
import pandas as pd
import sqlite3
import json
import re
import time
import logging
from datagouvfr_data_pipelines.utils.minio import (
    get_files_from_prefix,
)
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_INPI_USER,
    SECRET_INPI_PASSWORD,
)
from datagouvfr_data_pipelines.utils.minio import send_files, get_files
from datagouvfr_data_pipelines.utils.mattermost import send_message

DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/database/"
PATH_MINIO_RNE_DATA = "rne/database/"
LATEST_DATE_FILE = "latest_rne_date.json"
RNE_DATABASE_LOCATION = TMP_FOLDER + "rne.db"
MINIO_FLUX_DATA_PATH = "rne/flux/data/"



MINIO_DATA_PATH = "rne/flux/data/"

client = Minio(
    MINIO_URL,
    access_key=SECRET_MINIO_DATA_PIPELINE_USER,
    secret_key=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    secure=True,
)

yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_start_date_minio(ti):
    try:
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
            MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
            MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            list_files=[
                {
                    "source_path": PATH_MINIO_RNE_DATA,
                    "source_name": LATEST_DATE_FILE,
                    "dest_path": TMP_FOLDER,
                    "dest_name": LATEST_DATE_FILE,
                }
            ],
        )

        with open(f"{TMP_FOLDER}/latest_rne_date.json") as fp:
            data = json.load(fp)

        start_date = data["latest_date"]
        dt_sd = datetime.strptime(start_date, "%Y-%m-%d")
        start_date = datetime.strftime((dt_sd + timedelta(days=1)), "%Y-%m-%d")
        ti.xcom_push(key="start_date", value=start_date)
    except S3Error as e:
        if e.code == "NoSuchKey":
            logging.info(f"The file {PATH_MINIO_RNE_DATA + LATEST_DATE_FILE} "
                  f"does not exist in the bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}.")
            ti.xcom_push(key="start_date", value=None)
        else:
            raise Exception(f"An error occurred: {e}")
        
        
def create_db(ti):

    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    if start_date:
        return None

    if os.path.exists(RNE_DATABASE_LOCATION):
        os.remove(RNE_DATABASE_LOCATION)

    connection = sqlite3.connect(RNE_DATABASE_LOCATION)
    cursor = connection.cursor()
    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS rep_pp
                (
                    siren TEXT,
                    date_mise_a_jour TEXT,
                    date_de_naissance TEXT,
                    role TEXT,
                    nom TEXT,
                    nom_usage TEXT,
                    prenoms TEXT,
                    genre TEXT,
                    nationalite TEXT,
                    situation_matrimoniale TEXT,
                    pays TEXT,
                    code_pays TEXT,
                    code_postal TEXT,
                    commune TEXT,
                    code_insee_commune TEXT,
                    voie TEXT,
                    file TEXT
                )
        """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS rep_pm
                (
                    siren TEXT,
                    date_mise_a_jour TEXT,
                    denomination TEXT,
                    siren_pm TEXT,
                    role TEXT,
                    forme_juridique TEXT,
                    pays TEXT,
                    code_pays TEXT,
                    code_postal TEXT,
                    commune TEXT,
                    code_insee_commune TEXT,
                    voie TEXT,
                    file TEXT
                    )
                    """
    )
    create_index_db(cursor)
    connection.commit()
    connection.close()
        
        
def create_index_db(cursor):
    cursor.execute("""CREATE INDEX idx_siren_pp ON rep_pp (siren);""")
    cursor.execute("""CREATE INDEX idx_siren_pm ON rep_pm (siren);""")

        
def get_latest_db(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    if start_date is not None:
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
            MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
            MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            list_files=[
                {
                    "source_path": PATH_MINIO_RNE_DATA,
                    "source_name": "rne.db",
                    "dest_path": TMP_FOLDER,
                    "dest_name": "rne.db",
                }
            ],
        )

def process_flux_json_files(ti):
    json_daily_flux_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        prefix=MINIO_FLUX_DATA_PATH,
    )

    if not json_daily_flux_files:
        return None
    
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    if start_date is None:
        start_date = "0000-00-00"
    
    for file_name in sorted(json_daily_flux_files, reverse=True):
        date_match = re.search(r"rne_flux_(\d{4}-\d{2}-\d{2})", file_name)
        if date_match:
            file_date = date_match.group(1)
            if file_date > start_date:
                logging.info(f"Processing file {file_name} with date {file_date}")
                get_files(
                        MINIO_URL=MINIO_URL,
                        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
                        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
                        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
                        list_files=[
                            {
                                "source_path": "",
                                "source_name": f"{file_name}",
                                "dest_path": "",
                                "dest_name": f"{file_name}",
                            }
                        ]
                    )
                file_path = file_name
                inject_records_into_database(file_path)
                logging.info(f"File {file_path} processed and" 
                             " records injected into the database.")
                
    # Extract dates from the JSON file names and sort them
    dates = sorted(
        re.findall(r"rne_flux_(\d{4}-\d{2}-\d{2})", " ".join(json_daily_flux_files))
    )

    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last date saved: {last_date}")
    else:
        last_date = None
    ti.xcom_push(key="last_date", value=last_date)




                
def inject_records_into_database(file_path):
    with open(file_path, 'r') as file:
        logging.info(f"Processing file: {file_path}")
        for line in file:
            try:
                data = json.loads(line)
                
                for record in data:
                    #logging.info(f"/////////////{record}")
                    list_dirigeants_pp = extract_dirigeants_data(record)
           
                    insert_record(list_dirigeants_pp, file_path)
            except json.JSONDecodeError as e:
                raise Exception (f"JSONDecodeError: {e} in file {file_path}")


            
            
def insert_record(list_records, file_path):
    conn = sqlite3.connect(RNE_DATABASE_LOCATION)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM rep_pp")
    count = cursor.fetchone()[0]
    
    
    cursor.execute(f"SELECT * FROM rep_pp LIMIT 1")
    siren = cursor.fetchone()
    
    for dirigeant_pp in list_records:
        logging.info(f"++++++++++++++++++{count}")
        logging.info(f"??????????????{siren}")
        logging.info(f"{dirigeant_pp['siren']}")
        
        # If the record doesn't exist, insert it
        cursor.execute('''
            INSERT INTO rep_pp (siren, date_mise_a_jour, date_de_naissance, role, nom,
                                        nom_usage, prenoms, genre, nationalite, situation_matrimoniale,
                                        pays, code_pays, code_postal, commune, code_insee_commune, voie, file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dirigeant_pp['siren'], dirigeant_pp['date_mise_a_jour'], dirigeant_pp['date_de_naissance'],
            dirigeant_pp['role'], dirigeant_pp['nom'], dirigeant_pp['nom_usage'], dirigeant_pp['prenoms'],
            dirigeant_pp['genre'], dirigeant_pp['nationalite'], dirigeant_pp['situation_matrimoniale'],
            dirigeant_pp['pays'], dirigeant_pp['code_pays'], dirigeant_pp['code_postal'],
            dirigeant_pp['commune'], dirigeant_pp['code_insee_commune'], dirigeant_pp['voie'], file_path
        ))
        
        conn.commit()
    conn.close()
    
    
    
def extract_dirigeants_data(entity):
    """Extract and categorize "dirigeants" by type."""
    list_dirigeants_pp = []

    siren = entity.get("company", {}).get("siren")
    date_maj = entity.get("company", {}).get("updatedAt")
    dirigeants = (
        entity.get("company", {})
        .get("formality", {})
        .get("content", {})
        .get("exploitation", {})
        .get("composition", {})
        .get("pouvoirs", [])
    )

    for dirigeant in dirigeants:
        type_de_personne = dirigeant.get("typeDePersonne", None)

        if type_de_personne == "INDIVIDU":
            individu = dirigeant.get("individu", {}).get("descriptionPersonne", {})
            adresse_domicile = dirigeant.get("individu", {}).get(
                "adresseDomicile", {}
            )

            dirigeant_pp = {
                "siren": siren,
                "date_mise_a_jour": date_maj,
                "date_de_naissance": individu.get("dateDeNaissance", None),
                "role": individu.get("role", None),
                "nom": individu.get("nom", None),
                "nom_usage": individu.get("nomUsage", None),
                "prenoms": individu.get("prenoms", None),
                "genre": individu.get("genre", None),
                "nationalite": individu.get("nationalite", None),
                "situation_matrimoniale": individu.get(
                    "situationMatrimoniale", None
                ),
                "pays": adresse_domicile.get("pays", None),
                "code_pays": adresse_domicile.get("codePays", None),
                "code_postal": adresse_domicile.get("codePostal", None),
                "commune": adresse_domicile.get("commune", None),
                "code_insee_commune": adresse_domicile.get(
                    "codeInseeCommune", None
                ),
                "voie": adresse_domicile.get("voie", None),
            }
            dirigeant_pp["prenoms"] = " ".join(dirigeant_pp["prenoms"])

            list_dirigeants_pp.append(dirigeant_pp)

    return list_dirigeants_pp
    

def send_to_minio(list_files):
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=list_files,
    )


def retrieve_list_files(base_path):
    list_files = []
    for path, subdirs, files in os.walk(base_path):
        for name in files:
            formatted_file = {}
            formatted_file["source_path"] = f"{path}/"
            formatted_file["source_name"] = name
            formatted_file["dest_path"] = f"inpi/{path.replace(TMP_FOLDER, '')}/"
            formatted_file["dest_name"] = name
            list_files.append(formatted_file)
    return list_files


def upload_minio_synthese_files():
    list_files = retrieve_list_files(f"{TMP_FOLDER}synthese")
    print(list_files)
    send_to_minio(list_files)


def upload_minio_original_files():
    list_files_tc = retrieve_list_files(f"{TMP_FOLDER}flux-tc")
    list_files_stock = retrieve_list_files(f"{TMP_FOLDER}stock")
    send_to_minio(list_files_tc)
    send_to_minio(list_files_stock)


# Etape de processing des données en amont du lancement de l'enrichissement de la bdd sqlite
def concatFilesRep(
    days, type_file, name_concat, pattern
):  # "flux-tc", "_flux_rep", "*/*/*_5_rep.csv"
    for d in days:
        pathdate = TMP_FOLDER + type_file + "/" + d + "/"
        consofile = (
            TMP_FOLDER + "synthese/" + "-".join(d.split("/")) + name_concat + ".csv"
        )  # ?
        # get list files stock
        list_files = glob.glob(pathdate + pattern)
        list_files.sort()
        if len(list_files) > 0:
            with open(consofile, "wb") as fout:
                # first file:
                with open(list_files[0], "rb") as f:
                    fout.write(f.read())
                # now the rest:
                for ls in list_files[1:]:
                    with open(ls, "rb") as f:
                        try:
                            next(f)  # skip the header
                        except:
                            pass
                        fout.write(f.read())
        print(name_concat + " ok")



def uniformizeDf(df):
    for c in df.columns:
        df = df.rename(
            columns={
                c: unidecode.unidecode(
                    c.lower()
                    .replace(" ", "")
                    .replace(".", "")
                    .replace("_", "")
                    .replace('"', "")
                    .replace("'", "")
                )
            }
        )

    if "sigle" not in df.columns:
        df["sigle"] = ""
    # Do something
    df["siren"] = df.fillna("")["siren"]
    df["nom_patronymique"] = df.fillna("")["nompatronymique"]
    df["nom_usage"] = df.fillna("")["nomusage"]
    df["prenoms"] = df.fillna("")["prenoms"]
    df["nom_patronymique"] = df["nom_patronymique"].apply(
        lambda x: str(x).replace(",", "").lower()
    )
    df["nom_usage"] = df["nom_usage"].apply(lambda x: str(x).replace(",", "").lower())
    df["prenoms"] = df["prenoms"].apply(lambda x: str(x).replace(",", "").lower())
    df["datenaissance"] = df.fillna("")["datenaissance"]
    df["villenaissance"] = df.fillna("")["villenaissance"]
    df["paysnaissance"] = df.fillna("")["paysnaissance"]
    df["denomination"] = df.fillna("")["denomination"]
    df["siren_pm"] = df.fillna("")["siren1"]
    df["sigle"] = df.fillna("")["sigle"]
    df["qualite"] = df.fillna("")["qualite"]
    dfpp = df[df["type"].str.contains("Physique")][
        [
            "siren",
            "nom_patronymique",
            "nom_usage",
            "prenoms",
            "datenaissance",
            "villenaissance",
            "paysnaissance",
            "qualite",
        ]
    ]
    dfpm = df[df["type"].str.contains("Morale")][
        ["siren", "denomination", "siren_pm", "sigle", "qualite"]
    ]

    return dfpp, dfpm


# Toutes les dates après la date du stock initial
def update_db(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.today() - timedelta(days=1)
    delta = end - start  # as timedelta
    days = [
        datetime.strftime(start + timedelta(days=i), "%Y-%m-%d")
        for i in range(delta.days + 1)
    ]

    connection = sqlite3.connect(TMP_FOLDER + "inpi.db")
    cursor = connection.cursor()

    for d in days:
        consofile = TMP_FOLDER + "synthese/" + d + "_stock_rep.csv"
        # Manage stocks
        if os.path.exists(consofile):
            df = pd.read_csv(consofile, sep=";", dtype=str, warn_bad_lines="skip")
            dfpp, dfpm = uniformizeDf(df)
            print("loaded and uniformize")
            # Add new stock
            dfpp.set_index("siren").to_sql("rep_pp", con=connection, if_exists="append")
            dfpm.set_index("siren").to_sql("rep_pm", con=connection, if_exists="append")
            print(
                f"Stock processed : {str(dfpp.shape[0] + dfpm.shape[0])} added records"
            )

        consofile = TMP_FOLDER + "synthese/" + d + "_flux_rep.csv"
        if os.path.exists(consofile):
            df = pd.read_csv(consofile, sep=";", dtype=str, warn_bad_lines=True)
            dfpp, dfpm = uniformizeDf(df)
            dfpp.set_index("siren").to_sql("rep_pp", con=connection, if_exists="append")
            dfpm.set_index("siren").to_sql("rep_pm", con=connection, if_exists="append")
            print(
                f"Flux rep processed : {str(dfpp.shape[0] + dfpm.shape[0])} added records"
            )

        consofile = TMP_FOLDER + "synthese/" + d + "_flux_rep_nouveau_modifie.csv"
        if os.path.exists(consofile):
            df = pd.read_csv(consofile, sep=";", dtype=str, warn_bad_lines=True)
            dfpp, dfpm = uniformizeDf(df)
            dfpp.set_index("siren").to_sql("rep_pp", con=connection, if_exists="append")
            dfpm.set_index("siren").to_sql("rep_pm", con=connection, if_exists="append")
            print(
                f"Flux rep modified new processed : {str(dfpp.shape[0] + dfpm.shape[0])} added records"
            )

        consofile = TMP_FOLDER + "synthese/" + d + "_flux_rep_partant.csv"
        if os.path.exists(consofile):
            df = pd.read_csv(consofile, sep=";", dtype=str, warn_bad_lines=True)
            dfpp, dfpm = uniformizeDf(df)
            # Delete each rep
            result = 0
            for index, row in dfpp.iterrows():
                del_query = """
                    DELETE from rep_pp
                    WHERE
                        siren = ?
                    AND
                        nom_patronymique = ?
                    AND
                        prenoms = ?
                    AND
                        qualite = ?
                """
                cursor.execute(
                    del_query,
                    (
                        row["siren"],
                        row["nom_patronymique"],
                        row["prenoms"],
                        row["qualite"],
                    ),
                )
                result = result + cursor.rowcount
            connection.commit()
            for index, row in dfpm.iterrows():
                del_query = """DELETE from rep_pm where siren = ? AND siren_pm = ? AND qualite = ?"""
                cursor.execute(
                    del_query, (row["siren"], row["siren_pm"], row["qualite"])
                )
                result = result + cursor.rowcount
            connection.commit()
            print("Flux rep partant processed : " + str(result) + " deleted records")


def upload_minio_db():
    send_to_minio(
        [
            {
                "source_path": TMP_FOLDER,
                "source_name": "inpi.db",
                "dest_path": PATH_MINIO_INPI_DATA,
                "dest_name": "inpi.db",
            }
        ]
    )


def upload_minio_clean_db():
    send_to_minio(
        [
            {
                "source_path": TMP_FOLDER,
                "source_name": "inpi-clean.db",
                "dest_path": PATH_MINIO_INPI_DATA,
                "dest_name": "inpi-clean.db",
            }
        ]
    )


def check_emptiness():
    if len(glob.glob(TMP_FOLDER + "flux-tc/*")) != 0:
        return True
    else:
        if len(glob.glob(TMP_FOLDER + "stock/*")) != 0:
            return True
        else:
            return False




def get_latest_files_from_start_date(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.today() - timedelta(days=1)
    delta = end - start  # as timedelta
    days = [
        datetime.strftime(start + timedelta(days=i), "%Y-%m-%d")
        for i in range(delta.days + 1)
    ]
    for day in days:
        print("Retrieving inpi files from {}".format(day))
        get_latest_files_bash = BashOperator(
            task_id="get_latest_files_bash",
            bash_command=(
                f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}inpi/scripts/get.sh "
                f"{day} {SECRET_INPI_USER} {SECRET_INPI_PASSWORD} {TMP_FOLDER} "
            ),
        )
        get_latest_files_bash.execute(dict())


def upload_latest_date_inpi_minio(ti):
    latest_date = datetime.strftime((datetime.today() - timedelta(days=1)), "%Y-%m-%d")
    data = {}
    data["latest_date"] = latest_date
    with open(TMP_FOLDER + "latest_inpi_date.json", "w") as write_file:
        json.dump(data, write_file)

    send_to_minio(
        [
            {
                "source_path": TMP_FOLDER,
                "source_name": "latest_inpi_date.json",
                "dest_path": PATH_MINIO_INPI_DATA,
                "dest_name": "latest_inpi_date.json",
            }
        ]
    )
    ti.xcom_push(key="end_date", value=latest_date)


# Connect to database
def connect_to_db(db_location):
    db_conn = sqlite3.connect(db_location)
    db_cursor = db_conn.cursor()
    return db_conn, db_cursor


def normalize_string(string):
    if string is None:
        return None
    norm_string = (
        normalize("NFD", string.lower().strip())
        .encode("ascii", errors="ignore")
        .decode()
    )
    return norm_string


def normalize_date(date_string):
    if (date_string is None) or (date_string == ""):
        return ""

    date_patterns = ["%d-%m-%Y", "%Y-%m-%d", "%Y%m%d", "%d/%m/%Y"]
    for pattern in date_patterns:
        try:
            return datetime.strptime(date_string, pattern).strftime("%Y-%m-%d")
        except ValueError:
            pass
    print("ERROR", date_string)


def normalize_row_pp(row):
    return (
        normalize_string(row["siren"]) +
        normalize_string(row["nom_patronymique"]) +
        normalize_string(row["nom_usage"]) +
        normalize_string(row["prenoms"])
    )


def remove_useless_spaces(serie):
    serie = serie.apply(
        lambda x: " ".join(x.split(" ")[1:]) if x.split(" ")[0] == "" else x
    )
    serie = serie.apply(
        lambda x: " ".join(x.split(" ")[:-1]) if x.split(" ")[-1] == "" else x
    )
    return serie


def unique_qualites(qualite_string):
    # Sometimes, `qualite` might contain the same string repeated with different
    # format. Example: "administrateur, Administrateur"
    # Keep only : "administrateur"
    if not qualite_string:
        return ""
    # Split `qualite` string into multiple strings
    list_qualites = [s.strip() for s in qualite_string.split(",")]

    # Create dictionary with normalized qulity string as key and all corresponding
    # qualities as values (list of non-normalized qualities)

    qualites = {}

    for qualite in list_qualites:
        if normalize_string(qualite) in qualites:
            qualites[normalize_string(qualite)].append(qualite)
        else:
            qualites[normalize_string(qualite)] = [qualite]

    return ", ".join([qualites[qualite][-1] for qualite in qualites])


def unique_datenaissance(x):
    data = x.split(",")
    data.reverse()
    for d in data:
        if d != "":
            return d
    return ""


def deduplicate_and_clean_pp(df):
    df["id"] = df.apply(lambda row: normalize_row_pp(row), axis=1)
    # Process qualité
    df_qualite = df[["qualite", "id"]]
    df_qualite = (
        df_qualite.groupby(by=["id"])["qualite"]
        .apply(lambda x: ", ".join(x))
        .reset_index()
    )
    df_qualite["qualite"] = df_qualite["qualite"].apply(lambda x: unique_qualites(x))
    df = pd.merge(
        df.rename(columns={"qualite": "qualite_old"}), df_qualite, on="id", how="left"
    )

    # Process datenaissance
    df_naissance = df[["datenaissance", "id"]]
    df_naissance = (
        df_naissance.groupby(by=["id"])["datenaissance"]
        .apply(lambda x: ",".join(x))
        .reset_index()
    )
    df_naissance["datenaissance"] = df_naissance["datenaissance"].apply(
        lambda x: unique_datenaissance(x)
    )
    df = pd.merge(
        df.rename(columns={"datenaissance": "datenaissance_old"}),
        df_naissance,
        on="id",
        how="left",
    )
    df = df.drop_duplicates(subset=["id"], keep="last")

    return df.drop(["id", "datenaissance_old", "qualite_old"], axis=1)


def create_clean_db():
    DIRIG_CLEAN_DATABASE_LOCATION = TMP_FOLDER + "inpi-clean.db"

    if os.path.exists(DIRIG_CLEAN_DATABASE_LOCATION):
        os.remove(DIRIG_CLEAN_DATABASE_LOCATION)

    connection = sqlite3.connect(DIRIG_CLEAN_DATABASE_LOCATION)
    cursor = connection.cursor()
    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS rep_pp
                (
                    siren TEXT,
                    nom_patronymique TEXT,
                    nom_usage TEXT,
                    prenoms TEXT,
                    datenaissance TEXT,
                    villenaissance TEXT,
                    paysnaissance TEXT,
                    qualite TEXT
                )
        """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS rep_pm
                (siren TEXT, denomination TEXT, siren_pm TEXT, sigle TEXT, qualite TEXT)"""
    )
    connection.commit()


def create_index_clean_db():
    DIRIG_CLEAN_DATABASE_LOCATION = TMP_FOLDER + "inpi-clean.db"
    connection = sqlite3.connect(DIRIG_CLEAN_DATABASE_LOCATION)
    cursor = connection.cursor()

    cursor.execute("""CREATE INDEX idx_siren_pp ON rep_pp (siren);""")
    cursor.execute("""CREATE INDEX idx_siren_pm ON rep_pm (siren);""")
    cursor.execute(
        """CREATE INDEX idx_nom_patronymique_pp ON rep_pp (nom_patronymique);"""
    )
    cursor.execute("""CREATE INDEX idx_nom_usage_pp ON rep_pp (nom_usage);""")
    cursor.execute("""CREATE INDEX idx_siren_pm_pm ON rep_pm (siren_pm);""")
    connection.commit()


def preprocess_dirigeants_pp(query):
    cols = [column[0] for column in query.description]
    rep_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    rep_chunk["datenaissance"] = rep_chunk["datenaissance"].apply(
        lambda x: normalize_date(x)
    )
    rep_clean = (
        rep_chunk.groupby(
            by=[
                "siren",
                "nom_patronymique",
                "nom_usage",
                "prenoms",
                "datenaissance",
                "villenaissance",
                "paysnaissance",
            ]
        )["qualite"]
        .apply(lambda x: ", ".join(x))
        .reset_index()
    )
    rep_clean = deduplicate_and_clean_pp(rep_clean)
    return rep_clean


def clean_db_dirigeant_pp():
    DIRIG_DATABASE_LOCATION = TMP_FOLDER + "inpi.db"
    DIRIG_CLEAN_DATABASE_LOCATION = TMP_FOLDER + "inpi-clean.db"

    dirig_db_conn, dirig_db_cursor = connect_to_db(DIRIG_DATABASE_LOCATION)
    dirig_clean_db_conn, dirig_clean_db_cursor = connect_to_db(
        DIRIG_CLEAN_DATABASE_LOCATION
    )

    chunk_size = int(10000)
    for row in dirig_db_cursor.execute("""SELECT count(DISTINCT siren) FROM rep_pp;"""):
        nb_iter = int(int(row[0]) / chunk_size) + 1

    print(nb_iter, " iterations")

    start_time = time.time()
    for i in range(nb_iter):
        print(i, "--- %s seconds ---" % (time.time() - start_time))
        query = dirig_db_cursor.execute(
            f"""
            SELECT DISTINCT siren, nom_patronymique, nom_usage, prenoms,
            datenaissance, villenaissance, paysnaissance, qualite
            FROM rep_pp
            WHERE siren IN
                (
                SELECT DISTINCT siren
                FROM rep_pp
                WHERE siren != ''
                LIMIT {chunk_size}
                OFFSET {int(i * chunk_size)})
            """
        )
        dir_pp_clean = preprocess_dirigeants_pp(query)
        dir_pp_clean = dir_pp_clean.apply(lambda serie: remove_useless_spaces(serie))
        dir_pp_clean.to_sql(
            "rep_pp",
            dirig_clean_db_conn,
            if_exists="append",
            index=False,
        )
    del dir_pp_clean


def normalize_row_pm(row):
    return normalize_string(row["siren"]) + normalize_string(row["siren_pm"])


def deduplicate_and_clean_pm(df):
    df["id"] = df.apply(lambda row: normalize_row_pm(row), axis=1)
    # Process qualité
    df_qualite = df[["qualite", "id"]]
    df_qualite = (
        df_qualite.groupby(by=["id"])["qualite"]
        .apply(lambda x: ", ".join(x))
        .reset_index()
    )
    df_qualite["qualite"] = df_qualite["qualite"].apply(lambda x: unique_qualites(x))
    df = pd.merge(
        df.rename(columns={"qualite": "qualite_old"}), df_qualite, on="id", how="left"
    )

    df = df.drop_duplicates(subset=["id"], keep="last")

    return df.drop(["id", "qualite_old"], axis=1)


def preprocess_dirigeants_pm(query):
    cols = [column[0] for column in query.description]
    rep_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    rep_clean = (
        rep_chunk.groupby(
            by=[
                "siren",
                "denomination",
                "siren_pm",
                "sigle",
            ]
        )["qualite"]
        .apply(lambda x: ", ".join(x))
        .reset_index()
    )
    rep_clean = deduplicate_and_clean_pm(rep_clean)
    return rep_clean


def clean_db_dirigeant_pm():
    DIRIG_DATABASE_LOCATION = TMP_FOLDER + "inpi.db"
    DIRIG_CLEAN_DATABASE_LOCATION = TMP_FOLDER + "inpi-clean.db"

    dirig_db_conn, dirig_db_cursor = connect_to_db(DIRIG_DATABASE_LOCATION)
    dirig_clean_db_conn, dirig_clean_db_cursor = connect_to_db(
        DIRIG_CLEAN_DATABASE_LOCATION
    )

    chunk_size = int(10000)
    for row in dirig_db_cursor.execute("""SELECT count(DISTINCT siren) FROM rep_pm;"""):
        nb_iter = int(int(row[0]) / chunk_size) + 1

    print(nb_iter, " iterations")

    start_time = time.time()
    for i in range(nb_iter):
        print(i)
        print("--- %s seconds ---" % (time.time() - start_time))
        query = dirig_db_cursor.execute(
            f"""
            SELECT DISTINCT siren, denomination, siren_pm, sigle, qualite
            FROM rep_pm
            WHERE siren IN
                (
                SELECT DISTINCT siren
                FROM rep_pm
                WHERE siren != ''
                LIMIT {chunk_size}
                OFFSET {int(i * chunk_size)})
            """
        )
        dir_pm_clean = preprocess_dirigeants_pm(query)
        dir_pm_clean = dir_pm_clean.apply(lambda serie: remove_useless_spaces(serie))
        dir_pm_clean.to_sql(
            "rep_pm",
            dirig_clean_db_conn,
            if_exists="append",
            index=False,
        )
    del dir_pm_clean


def notification_mattermost(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    end_date = ti.xcom_pull(key="end_date", task_ids="upload_latest_date_inpi")
    send_message(
        f"Données INPI mise à jour de {start_date} à {end_date} sur Minio "
        f"- Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}",
    )
