
from airflow.models import Variable
import frontmatter
import glob
import requests
import random
import pandas as pd
import re

from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MATTERMOST_TMPAPIGOUV
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}migration_apigouv"
GRIST_TOKEN = Variable.get("GRIST_APIGOUV_TOKEN", "metric")
DATAGOUV_TOKEN = Variable.get("DATAGOUV_SECRET_API_KEY")
DATAGOUV_URL = "https://www.data.gouv.fr"
property_grist = "lien_datagouv_prod"
doc_id = "fKtVwXxfqNfw"

headers_grist = {
    'Authorization': 'Bearer ' + GRIST_TOKEN
}

headers_dgv = {
    "X-API-KEY": DATAGOUV_TOKEN
}


def get_df(url):
    r = requests.get(url, headers=headers_grist)
    arr = []
    for item in r.json()["records"]:
        mydict = item["fields"]
        mydict["id"] = item["id"]
        arr.append(mydict)
    df = pd.DataFrame(arr)
    return df


def import_api_to_grist(ti):
    files = glob.glob(TMP_FOLDER + "/api.gouv.fr/_data/api/*")
    list_apis = []
    for f in files:
        post = frontmatter.load(f)
        post["description"] = post["content_intro"] + "\n" + post.content if "content_intro" in post else post.content
        post["description"] = post["tagline"] + "\n" + post["description"] if "tagline" in post else post["description"]
        post["description"] = post["description"] + "\n[Lien de monitoring](" + post["monitoring_link"] + ")" if "monitoring_link" in post and post["monitoring_link"] is not None else post["description"]
        post["description"] = post["description"] + "\n" + post["stats_detail_resume"] if "stats_detail_resume" in post else post["description"]
        post["description"] = post["description"] + "\n" + post["stats_detail_description"] if "stats_detail_description" in post else post["description"]
        post["description"] = post["description"] + "\n[Lien des statistiques de l'api](" + post["stats_detail_link"] + ")" if "stats_detail_link" in post else post["description"]
        post["description"] = post["description"] + "\n[Lien des documentation supplémentaire](" + post["doc_tech_external"] + ")" if "doc_tech_external" in post else post["description"]

        post = post.to_dict()
        if "content_intro" in post:
            post.pop('content_intro')
        if "tagline" in post:
            post.pop('tagline')
        if "content" in post:
            post.pop('content')
        post["lien_api_gouv_fr"] = "https://api.gouv.fr/les-api/" + f.split("/")[-1].replace(".md", "")
        list_apis.append(post)

    files = glob.glob(TMP_FOLDER + "/api.gouv.fr/_data/api/*")
    for list_api in list_apis:
        try:
            org = frontmatter.load("/Volumes/OnlyData/projects/apigouv-migration/api.gouv.fr/_data/producteurs/" + list_api["producer"] + ".md")
            org = org.to_dict()
            list_api["producer_api"] = org["name"]
        except:
            print(list_api["title"])

    for l in list_apis:
        if l["is_open"] == -1:
            l["is_restricted"] = True
        if l["is_open"] == 1:
            l["is_restricted"] = False

    for l in list_apis:
        l["base_api_url"] = l["external_site"] if "external_site" in l else None
        l["authorization_request_url"] = l["datapass_link"] if "datapass_link" in l else None
        l["tags"] = l["keywords"] if "keywords" in l else None
        l["rate_limiting"] = l["rate_limiting_resume"] if "rate_limiting_resume" in l else None
        if "rate_limiting_description" in l and l["rate_limiting_description"]:
            l["rate_limiting"] += "\n" + l["rate_limiting_description"]
        if "rate_limiting_link" in l and l["rate_limiting_link"]:
            l["rate_limiting"] += "\n" + l["rate_limiting_link"]
        l["contact_point"] = l["contact_link"] if "contact_link" in l else None
        l["endpoint_description_url"] = l["doc_tech_link"] if "doc_tech_link" in l else None
        l["topics"] = l["themes"] if "themes" in l else None
        l["availability"] = l["uptime"] if "uptime" in l else None

    cols_to_keep = [
        "lien_api_gouv_fr", "title", "base_api_url", "is_restricted", "authorization_request_url",
        "partners", "producer_api", "producer_data", "producer_url", "tags", "rate_limiting", "contact_point",
        "endpoint_description_url", "themes", "availability", "description"
    ]
    apis = []
    for l in list_apis:
        mydict = {}
        for item in l:
            if item in cols_to_keep:
                mydict[item] = l[item]
        mydict["api_id"] = str(random.getrandbits(128))
        apis.append(mydict)

    api_url = "https://grist.numerique.gouv.fr/api/"
    apikey = GRIST_TOKEN
    headers = {
        "Authorization": "Bearer " + apikey
    }

    df = get_df("https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_all/records")

    list_sources = [item["title"] for item in apis]
    list_grist = df["title"].to_list()
    list_sources = [item for item in list_sources if item not in list_grist]
    apis = [item for item in apis if item["title"] in list_sources]
    apis = [item for item in apis if item["title"] != "API XXX"]
    print("ajout de " + str(len(apis)) + " apis")

    cols_tables_ids = [
        {
            "table": "Api_all",
            "cols": [
                "api_id", "lien_api_gouv_fr", "title", "base_api_url", "is_restricted", "authorization_request_url",
                "producer_api", "producer_data", "producer_url", "rate_limiting", "contact_point",
                "endpoint_description_url", "availability"
            ],
        },
        {
            "table": "Api_partners",
            "cols": ["api_id", "title", "partners"],
        },
        {
            "table": "Api_tags",
            "cols": ["api_id", "title", "tags"],
        },
        {
            "table": "Api_themes",
            "cols": ["api_id", "title", "themes"],
        },
        {
            "table": "Api_description",
            "cols": ["api_id", "title", "description"],
        },
    ]

    for cti in cols_tables_ids:
        print(cti)
        table = {
            "tables": [
                {
                    "id": cti["table"],
                    "columns": []
                }
            ]
        }
        for col in cti["cols"]:
            table["tables"][0]["columns"].append(
                {
                    "id": col,
                    "fields": {
                        "label": col
                    }
                }
            )
        records = {
            "records": []
        }

        if cti["table"] in ["Api_partners", "Api_tags", "Api_themes"]:
            for a in apis:
                records = {
                    "records": []
                }  
                if cti["table"] == "Api_partners" and "partners" in a and a["partners"] == a["partners"] and a["partners"] is not None:
                    print(a["title"])
                    for item in a["partners"]:
                        mydict = {}
                        mydict["api_id"] = a["api_id"]
                        mydict["title"] = a["title"]
                        mydict["partners"] = item
                        records["records"].append(
                            {
                                "fields": mydict
                            }
                        )
                        print(mydict)
                    r2 = requests.post(
                        api_url + "docs/" + doc_id + "/tables/" + cti["table"] + "/records",
                        headers=headers,
                        json=records
                    )

                if cti["table"] == "Api_tags" and "tags" in a and a["tags"] == a["tags"] and a["tags"] is not None:
                    for item in a["tags"]:
                        mydict = {}
                        mydict["api_id"] = a["api_id"]
                        mydict["title"] = a["title"]
                        mydict["tags"] = item
                        records["records"].append(
                            {
                                "fields": mydict
                            }
                        )
                    r2 = requests.post(
                        api_url + "docs/" + doc_id + "/tables/" + cti["table"] + "/records",
                        headers=headers,
                        json=records
                    )

                if cti["table"] == "Api_themes" and "themes" in a and a["themes"] == a["themes"] and a["themes"] is not None:
                    for item in a["themes"]:
                        mydict = {}
                        mydict["api_id"] = a["api_id"]
                        mydict["title"] = a["title"]
                        mydict["themes"] = item
                        records["records"].append(
                            {
                                "fields": mydict
                            }
                        )
                    r2 = requests.post(
                        api_url + "docs/" + doc_id + "/tables/" + cti["table"] + "/records",
                        headers=headers,
                        json=records
                    )

        print(cti["table"])
        if cti["table"] in ["Api_all", "Api_description"]:
            records = {
                "records": []
            }
            for a in apis:
                mydict = {}
                for item in a:
                    if item in cti["cols"]:
                        mydict[item] = a[item]
                records["records"].append(
                    {
                        "fields": mydict
                    }
                )
            print(records)
            r2 = requests.post(
                api_url + "docs/" + doc_id + "/tables/" + cti["table"] + "/records",
                headers=headers,
                json=records
            )
            print(r2.json())
    list_sources = [item for item in list_sources if item != "API XXX"]
    ti.xcom_push(key="list_sources", value=list_sources)


def publish_api_to_datagouv(ti):
    df = get_df("https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_all/records")
    df_tags = get_df("https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_tags/records")
    df_themes = get_df("https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_themes/records")
    df_desc = get_df("https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_description/records")
    df_pc = get_df("https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_publics_cibles/records")
    df_jdd = get_df("https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_jeu_de_donnees/records")

    apikos = []

    for _, row in df[df["Ok_pour_migrer"] == True].iterrows():   
        try:
            print("----")
            print(row["title"])
            r = requests.get(row["producer_url"].replace("/fr/", "/api/1/"))
            org_id = r.json()["id"]
            r = requests.get(DATAGOUV_URL + "/api/1/organizations/" + org_id + "/contacts")
            # try:
            #     contacts = r.json()["data"]
            # except:
            #     print(f"org {org_id} not found on {DATAGOUV_URL}, replacing by DINUM")
            #     org_id = "57fe2a35c751df21e179df72"
            #     r = requests.get(DATAGOUV_URL + "/api/1/organizations/direction-interministerielle-du-numerique/contacts")
            #     contacts = r.json()["data"]
            contacts = r.json()["data"]

            contact_exist = False
            for contact in contacts:
                if contact["email"] == row["contact_point"]:
                    contact_id = contact["id"]
                    contact_exist = True
                if contact["contact_form"] == row["contact_point"]:
                    contact_id = contact["id"]
                    contact_exist = True
            if not contact_exist:
                if is_valid_email(row["contact_point"]):
                    print("post " + row["contact_point"] + " for orga " + org_id)
                    r = requests.post(
                        DATAGOUV_URL + "/api/1/contacts",
                        json={
                            "name": row["contact_point"],
                            "email": row["contact_point"],
                            "organization": org_id
                        },
                        headers=headers_dgv
                    )
                    contact_id = r.json()["id"]
                    contact_exist = True
                elif is_valid_url(row["contact_point"]):
                    print("post " + row["contact_point"] + " for orga " + org_id)
                    r = requests.post(
                        DATAGOUV_URL + "/api/1/contacts",
                        json={
                            "name": "Contact",
                            "contact_form": row["contact_point"],
                            "organization": org_id
                        },
                        headers=headers_dgv
                    )
                    contact_id = r.json()["id"]
                    contact_exist = True
                else:
                    print("pas de contact valide")

            mydict = {}
            mydict["title"] = row["title"]
            if row["base_api_url"] != "":
                mydict["base_api_url"] = row["base_api_url"]
            # else:
            #     mydict["base_api_url"] = "https://www.data.gouv.fr"
            if row["is_restricted"] == "False":
                mydict["is_restricted"] = False
            else: 
                mydict["is_restricted"] = True
            if row["authorization_request_url"] != "":
                mydict["authorization_request_url"] = row["authorization_request_url"]
            if row["documentation_metier"] != "":
                mydict["business_documentation_url"] = row["documentation_metier"]
            mydict["rate_limiting"] = row["rate_limiting"]
            if row["swagger_url"] != "":
                mydict["endpoint_description_url"] = row["swagger_url"]
            if row["availability"]:
                mydict["availability"] = float(row["availability"])
            mydict["extras"] = {}
            mydict["extras"]["availability_url"] = row["availability_url"]
            mydict["extras"]["is_franceconnect"] = row["is_FC"]
            tags = []
            if df_tags[df_tags["api_id"] == row["api_id"]].shape[0] > 0:
                for index2, row2 in df_tags[df_tags["api_id"] == row["api_id"]].iterrows():
                    if row2["tags"]:
                        tags.append(row2["tags"])
            if df_themes[df_themes["api_id"] == row["api_id"]].shape[0] > 0:
                for index2, row2 in df_themes[df_themes["api_id"] == row["api_id"]].iterrows():
                    if row2["themes"]:
                        tags.append(row2["themes"])
            mydict["tags"] = tags
            mydict["description"] = df_desc[df_desc["api_id"] == row["api_id"]].iloc[0][
                "description_sans_tagline_result"
            ]
            pc = []
            for _, row2 in df_pc[df_pc["api_id"] == row["api_id"]].iterrows():
                if row2 is not None:
                    mydict2 = {}
                    mydict2["name"] = row2["Public_Cible"]
                    mydict2["has_access"] = row2["Eligible"]
                    mydict2["description"] = row2["phrase"]
                    pc.append(mydict2)
            mydict["extras"]["public_cible"] = pc
            datasets = []
            for _, row2 in df_jdd[df_jdd["api_id"] == row["api_id"]].iterrows():
                datasets.append(row2["Jeu_de_donnees_data_gouv_fr"])

            for dataset in datasets:
                r = requests.get(DATAGOUV_URL + "/api/1/datasets/" + dataset)
                if r.status_code == 404:
                    datasets = []
                    print(f"dataset {dataset} not found in {DATAGOUV_URL}")

            mydict["datasets"] = datasets
            if contact_exist:
                mydict["contact_point"] = contact_id
            mydict["organization"] = org_id

            if row[property_grist] is None or row[property_grist] == '' or row[property_grist] != row[property_grist]:
                print("post " + row["title"] + " in dgv")
                r = requests.post(DATAGOUV_URL + "/api/1/dataservices/", headers=headers_dgv, json=mydict)

                payload = {
                    "records": [
                        {
                            "id": row["id"],
                            "fields": {}
                        }
                    ]
                }
                payload["records"][0]["fields"][property_grist] = r.json()["self_web_url"]
                print("patch " + row["title"] + " in grist")
                r = requests.patch(
                    "https://grist.numerique.gouv.fr/api/docs/" + doc_id + "/tables/Api_all/records",
                    json=payload,
                    headers=headers_grist
                )

            else:
                print("link existing")
                id_dgv = row[property_grist].split("/")[-2]
                print("patch " + row["title"] + " in dgv")
                r = requests.patch(
                    DATAGOUV_URL + "/api/1/dataservices/" + id_dgv + "/",
                    headers=headers_dgv,
                    json=mydict,
                )
        except:
            print(f"Problem with - {row['title']}")
            apikos.append(row['title'])
            try:
                print(r.json())
            except:
                pass
        ti.xcom_push(key="apikos", value=apikos)


def publish_mattermost(ti):
    list_sources = ti.xcom_pull(key="list_sources", task_ids="import_api_to_grist")
    apikos = ti.xcom_pull(key="apikos", task_ids="publish_api_to_datagouv")
    list_sources_str = ""
    for ls in list_sources:
        list_sources_str += "- " + ls + "\n"
    if len(list_sources) > 0:
        send_message(
            ":mega: @magali.bouvat Nouvelles APIs ajoutées au Grist (à la fin du tableur) \n" + list_sources_str,
            MATTERMOST_TMPAPIGOUV
        )
    apikos_str = ""
    for apiko in apikos:
        apikos_str += "\n - " + apiko
    if len(apikos) > 0:
        send_message(
            ":mega: Problème avec l'intégration dans demo.data.gouv.fr de certaines apis :" + apikos_str,
            MATTERMOST_TMPAPIGOUV
        )


def is_valid_email(email):
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email) is not None


def is_valid_url(url):
    motif_url = re.compile(
        r'^(https?|ftp)://'
        r'(([A-Za-z0-9-]+\.)+[A-Za-z]{2,6})'
        r'(/[A-Za-z0-9._~:/?#[@!$&\'()*+,;=%-]*)?$'
    )
    return bool(motif_url.match(url))
