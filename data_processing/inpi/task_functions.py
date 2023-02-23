from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
from unicodedata import normalize
import os
from minio import Minio
import glob
import unidecode
import pandas as pd
import sqlite3
import json
import time
from dag_datagouv_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    SECRET_INPI_USER,
    SECRET_INPI_PASSWORD,
)
from dag_datagouv_data_pipelines.utils.minio import send_files, get_files
from dag_datagouv_data_pipelines.utils.mattermost import send_message

DAG_FOLDER = 'dag_datagouv_data_pipelines/data_processing/'
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}inpi/"
PATH_MINIO_INPI_DATA = "inpi/"

client = Minio(
    MINIO_URL,
    access_key=SECRET_MINIO_DATA_PIPELINE_USER,
    secret_key=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    secure=True,
)

yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def send_to_minio(list_files):
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
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


def get_latest_db(ti):
    get_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": PATH_MINIO_INPI_DATA,
                "source_name": "inpi.db",
                "dest_path": TMP_FOLDER,
                "dest_name": "inpi.db",
            }
        ],
    )

    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.today() - timedelta(days=1)
    delta = end - start  # as timedelta
    days = [
        datetime.strftime(start + timedelta(days=i), "%Y/%m/%d")
        for i in range(delta.days + 1)
    ]

    concatFilesRep(days, "stock", "_stock_rep", "*/*/*_5_rep.csv")
    concatFilesRep(days, "flux-tc", "_flux_rep", "*/*/*_5_rep.csv")
    concatFilesRep(
        days,
        "flux-tc",
        "_flux_rep_nouveau_modifie",
        "*/*/*6_rep_nouveau_modifie_EVT.csv",
    )
    concatFilesRep(days, "flux-tc", "_flux_rep_partant", "*/*/*7_rep_partant_EVT.csv")


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


def get_start_date_minio(ti):
    get_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": PATH_MINIO_INPI_DATA,
                "source_name": "latest_inpi_date.json",
                "dest_path": TMP_FOLDER,
                "dest_name": "latest_inpi_date.json",
            }
        ],
    )

    with open(f"{TMP_FOLDER}/latest_inpi_date.json") as fp:
        data = json.load(fp)

    start_date = data["latest_date"]
    dt_sd = datetime.strptime(start_date, "%Y-%m-%d")
    start_date = datetime.strftime((dt_sd + timedelta(days=1)), "%Y-%m-%d")
    ti.xcom_push(key="start_date", value=start_date)


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
                f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}/inpi/scripts/get.sh "
                f"{day} {SECRET_INPI_USER} {SECRET_INPI_PASSWORD} {TMP_FOLDER}"
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
                "dest_name": "latest_inpi_date.db",
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
        f"- Bucket {MINIO_BUCKET_DATA_PIPELINE}",
    )
