from datagouvfr_data_pipelines.utils.download import download_files
import os
import pandas as pd
import json
import datetime
from datetime import datetime, timedelta
import random
import requests
import re
import time
from requests.exceptions import SSLError
from datagouvfr_data_pipelines.utils.mattermost import send_message
import logging
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    AUTH_RNE,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/flux/"
DATADIR = f"{TMP_FOLDER}data"
ZIP_FILE_PATH = f"{TMP_FOLDER}rne.zip"
EXTRACTED_FILES_PATH = f"{TMP_FOLDER}extracted/"
DEFAULT_START_DATE = "2023-07-01"
RNE_API_DIFF_URL = "https://registre-national-entreprises.inpi.fr/api/companies/diff?"


def get_last_json_file_date(folder_path=TMP_FOLDER):
    json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
    if not json_files:
        return None
    # Extract dates from the JSON file names
    date_pattern = r"rne_flux_(\d{4}-\d{2}-\d{2})"
    dates = [re.search(date_pattern, f).group(1) for f in json_files]
    # Sort the dates and get the last one
    dates.sort()
    last_date = dates[-1]
    return last_date


def get_daily_flux_rne(
    start_date: str,
    end_date: str,
    session,
    url: str,
    auth: list[dict],
    token: str | None,
):
    if not token:
        # If no token is provided, fetch a new one
        logging.info("Getting new token...")
        token = get_new_token(session, url, auth)
    headers = {"Authorization": f"Bearer {token}"}

    last_siren = None  # Initialize last_siren

    json_file_name = f"flux-rne/rne_flux_{start_date}.json"

    with open(json_file_name, "w") as json_file:
        while True:
            url = f"{RNE_API_DIFF_URL}from={start_date}&to={end_date}&pageSize=100"

            if last_siren:
                url += f"&searchAfter={last_siren}"
            try:
                r = make_api_request(session, url, auth, headers)
                page_data = r.json()
                if page_data:
                    last_company_in_response = page_data[
                        -1
                    ]  # Get the last object in the response
                    last_siren = last_company_in_response.get("company", {}).get(
                        "siren"
                    )
                    print(f"*******{last_siren}")
                    # Save the response data to the JSON file
                    json.dump(page_data, json_file)
                    json_file.flush()
                else:
                    last_siren = None
                    print(f"*******Closing file: {json_file_name}")
                    break
            except Exception as e:
                logging.error(f"Error occurred during the API request: {e}")
                # If the API request failed, delete the current JSON file and break the loop
                print(f"Deleting file: {json_file_name}")
                os.remove(json_file_name)
                break
    json_file.close()


def get_new_token(session, url: str, auth: list[dict]) -> str | None:
    """Gets a new access token from the RNE API.

    Args:
        url: The URL of the RNE token endpoint.
        auth: The authentication credentials to use when requesting a new token.

    Returns:
        A string containing the new access token, or `None` if an error occurred.
    """

    try:
        # Select a random authentication method from the `AUTH` list.
        selected_auth = random.choice(auth)
        logging.info(f"Authentification account used: {selected_auth['user_name']}")

        # Make a POST request to the RNE token endpoint with the selected authentication method.
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


def make_api_request(session, url, auth, headers, max_retries=10):
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

    for attempt in range(max_retries + 1):
        logging.info(f"Making API call try : {attempt}")

        try:
            response = session.get(url, headers=headers)
            response.raise_for_status()
            return response

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


def get_every_daily_flux(
    url, auth=AUTH_RNE, 
    token=None, 
    folder_path=TMP_FOLDER
    ):
    # Create a persistent session
    session = create_persistent_session()

    # Get the start and end date
    start_date = get_last_json_file_date(folder_path) or DEFAULT_START_DATE
    end_date = datetime.now().strftime("%Y-%m-%d")

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        start_date_formatted = current_date.strftime("%Y-%m-%d")
        next_day = current_date + timedelta(days=1)
        next_day_formatted = next_day.strftime("%Y-%m-%d")

        get_daily_flux_rne(
            start_date_formatted, next_day_formatted, session, url, auth, token
        )

        current_date = next_day
