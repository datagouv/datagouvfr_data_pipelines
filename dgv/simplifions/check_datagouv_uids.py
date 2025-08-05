import requests
import os

GRIST_API_URL = os.getenv("GRIST_API_URL")
SECRET_GRIST_API_KEY = os.getenv("SECRET_GRIST_API_KEY")
GRIST_DOC_ID = "c5pt7QVcKWWe"
DATAGOUV_BASE_URL = "https://www.data.gouv.fr"


def request_grist_table(table_id: str, filter: str = None) -> list[dict]:
    r = requests.get(
        GRIST_API_URL + f"docs/{GRIST_DOC_ID}/tables/{table_id}/records",
        headers={
            'Authorization': 'Bearer ' + SECRET_GRIST_API_KEY,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
        params={"filter": filter} if filter else None,
    )
    r.raise_for_status()
    return [row["fields"] for row in r.json()["records"]]

def request_datagouv_api(type: str, uid: str) -> dict:
    r = requests.get(f"{DATAGOUV_BASE_URL}/api/1/{type}/{uid}/")
    return r


apidatas = request_grist_table(table_id="Apidata")

relevant_apidatas = [apidata for apidata in apidatas if apidata.get("UID_data_gouv")]

for apidata in relevant_apidatas:
    datagouv_uid = apidata.get("UID_data_gouv")
    type = apidata.get("Type")
    datagouv_type = "dataservices" if type == "API" else "datasets"
    datagouv_response = request_datagouv_api(datagouv_type, datagouv_uid)
    if datagouv_response.status_code == 200:
        print(f"OK - Datagouv UID: {datagouv_uid}, Type: {datagouv_type}")
    else:
        print(f"CODE {datagouv_response.status_code} - Datagouv UID: {datagouv_uid}, Type: {datagouv_type}, Status code: {datagouv_response.status_code}")