import os
import json
from datetime import datetime, timedelta
import random
import requests
import re
import time
import logging
from typing import List, Dict, Union
from requests.exceptions import SSLError
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.utils.minio import (
    get_files_from_prefix,
)
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    AUTH_RNE,
)


TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/flux/"
DATADIR = f"{TMP_FOLDER}data"
DEFAULT_START_DATE = "2023-07-01"
RNE_API_DIFF_URL = "https://registre-national-entreprises.inpi.fr/api/companies/diff?"
RNE_API_TOKEN_URL = "https://registre-national-entreprises.inpi.fr/api/sso/login"
MINIO_DATA_PATH = "rne/flux/data/"


def get_last_json_file_date():
    json_daily_flux_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        prefix=MINIO_DATA_PATH,
    )

    if not json_daily_flux_files:
        return None

    # Extract dates from the JSON file names and sort them
    dates = sorted(
        re.findall(r"rne_flux_(\d{4}-\d{2}-\d{2})", " ".join(json_daily_flux_files))
    )

    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last date saved: {last_date}")
        return last_date
    else:
        return None


def get_last_siren_in_page(page_data):
    return page_data[-1].get("company", {}).get("siren") if page_data else None


def get_and_save_daily_flux_rne(
    start_date: str,
    end_date: str,
    session,
    auth: List[Dict],
):
    json_file_name = f"rne_flux_{start_date}.json"
    json_file_path = f"{DATADIR}/{json_file_name}"

    if not os.path.exists(DATADIR):
        logging.info(f"********** Creating {DATADIR}")
        os.makedirs(DATADIR)

    last_siren = None  # Initialize last_siren

    with open(json_file_path, "w") as json_file:
        logging.info(f"****** Opening file: {json_file_path}")
        while True:
            try:
                page_data, last_siren = make_api_request(
                    session, auth, start_date, end_date, last_siren
                )

                if page_data:
                    json.dump(page_data, json_file)
                    json_file.flush()
                else:
                    break

            except Exception as e:
                # If the API request failed, delete the current
                # JSON file and break the loop
                logging.error(f"Error occurred during the API request: {e}")
                logging.info(f"****** Deleting file: {json_file_path}")
                os.remove(json_file_path)
                break
    logging.info(f"****** Closing file: {json_file_path}")
    json_file.close()
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{json_file_name}",
                "dest_path": MINIO_DATA_PATH,
                "dest_name": f"{json_file_name}",
            },
        ],
    )
    logging.info(f"****** Sent file to MinIO: {json_file_name}")


def get_new_token(session, url: str, auth: List[Dict]) -> Union[str, None]:
    """Gets a new access token from the RNE API.g

    Args:
        url: The URL of the RNE token endpoint.
        auth: The authentication credentials to use when requesting a new token.

    Returns:
        A string containing the new access token, or `None` if an error occurred.
    """

    try:
        # Select a random authentication method from the `AUTH` list.
        selected_auth = random.choice(auth)
        logging.info(f"Authentification account used: {selected_auth['username']}")

        # Make a POST request to the RNE token endpoint
        # with the selected authentication method.
        response = session.post(url, json=selected_auth)

        # Raise an exception if the response status code is not 200 OK.
        response.raise_for_status()

        # Extract the access token from the response JSON payload.
        token = response.json()["token"]

        # Print a message indicating that a new token has been received.
        logging.info("New token received...")

        # Return the access token.
        return token

    # Handle SSL errors.
    except SSLError as err:
        logging.warning(f"Unexpected EOF occurred in violation of protocol: {err}")
        time.sleep(600)

    # Handle other exceptions.
    except Exception as err:
        logging.error(f"An error occurred when trying to get new token: {err}")


def create_persistent_session():
    """Create a session with a custom HTTP adapter for max retries."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=20)
    session.mount("http://", adapter)

    return session


def make_api_request(
    session,
    auth,
    start_date,
    end_date,
    last_siren=None,
    max_retries=10,
):
    """Makes an API request and retries it up to max_retries times if it fails,
    and gets a new token if the return is access denied.

    Args:
      url: The URL of the API endpoint.
      payload: The payload to send with the API request.
      headers: The headers to send with the API request.
      max_retries: The maximum number of times to retry the API request.

    Returns:
      A response object from the API.

    Raises:
      Exception: If the API request fails after max_retries retries.
    """

    url = f"{RNE_API_DIFF_URL}from={start_date}&to={end_date}&pageSize=100"
    if last_siren:
        url += f"&searchAfter={last_siren}"

    for attempt in range(max_retries + 1):
        if attempt > 0:
            logging.info(f"Making API call try : {attempt}")
        try:
            logging.info("Getting new token...")
            token = get_new_token(session, RNE_API_TOKEN_URL, auth)
            headers = {"Authorization": f"Bearer {token}"}
            response = session.get(url, headers=headers)
            response.raise_for_status()
            response = response.json()
            last_siren = get_last_siren_in_page(response)
            return response, last_siren

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 or e.response.status_code == 401:
                # Get a new token and retry the request.
                token = get_new_token(session, url, auth)
                headers["Authorization"] = f"Bearer {token}"
                logging.info("Got a new access token and retrying...")
            elif e.response.status_code == 500:
                # Check if the response content contains the memory exhaustion message
                if "Allowed memory size of" in str(e.response.content):
                    url = url.replace("pageSize=100", "pageSize=50")
                    logging.info(f"*****Memory Error changing size: {e}")
                else:
                    logging.info(f"*****Error HTTP: {e}")
                    time.sleep(60)
            else:
                logging.info(f"*****Error HTTP: {e}")
                time.sleep(60)
        except Exception as e:
            logging.error(f"Error occurred while making API request: {e}")
            if attempt < max_retries:
                # Retry the request after a backoff period.
                time.sleep(60)
            else:
                raise Exception(
                    "Max retries reached. Unable to establish a connection."
                )


def get_every_day_flux(
    auth=AUTH_RNE,
    **kwargs,
):
    # Create a persistent session
    session = create_persistent_session()

    # Get the start and end date
    start_date = get_last_json_file_date() or DEFAULT_START_DATE
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logging.info(f"********* Start date: {start_date}")
    logging.info(f"********* End date: {end_date}")

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        start_date_formatted = current_date.strftime("%Y-%m-%d")
        next_day = current_date + timedelta(days=1)
        next_day_formatted = next_day.strftime("%Y-%m-%d")

        get_and_save_daily_flux_rne(
            start_date_formatted, next_day_formatted, session, auth
        )

        current_date = next_day

    kwargs["ti"].xcom_push(key="rne_flux_start_date", value=start_date)
    kwargs["ti"].xcom_push(key="rne_flux_end_date", value=end_date)


def send_notification_mattermost(**kwargs):
    rne_flux_start_date = kwargs["ti"].xcom_pull(
        key="rne_flux_start_date", task_ids="get_every_day_flux"
    )
    rne_flux_end_date = kwargs["ti"].xcom_pull(
        key="rne_flux_end_date", task_ids="get_every_day_flux"
    )
    send_message(
        f"Données flux RNE mise à jour sur Minio "
        f"- Bucket {MINIO_BUCKET_DATA_PIPELINE} :"
        f"\n - Date début flux : {rne_flux_start_date} "
        f"\n - Date fin flux : {rne_flux_end_date} "
    )
