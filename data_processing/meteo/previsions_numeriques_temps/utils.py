import json
import logging
import os
import random
import time

import requests
from airflow.models import Variable

from datagouvfr_data_pipelines.config import METEO_PNT_APPLICATION_ID
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry


class MeteoClient(object):
    # code is courtesy of Météo France: https://portail-api.meteofrance.fr/web/fr/faq

    def __init__(self):
        self.session = requests.Session()
        self.token_url = "https://portail-api.meteofrance.fr/token"
        # this is just an example URL to check potential expiration
        self.test_expiration_url = (
            "https://public-api.meteofrance.fr/previnum/"
            "DPPaquetWAVESMODELS/models/MFWAM/grids"
        )

    def request(self, method, url, **kwargs):
        time.sleep(0.2)
        # First request will always need to obtain a token first
        if 'Authorization' not in self.session.headers:
            self.obtain_token()
        # Optimistically attempt to dispatch request
        response = self.session.request(method, url, **kwargs)
        if self.token_has_expired(response):
            # We got an 'Access token expired' response => refresh token
            self.obtain_token()
            # Re-dispatch the request that previously failed
            response = self.session.request(method, url, **kwargs)
        if response.status_code == 429:
            logging.warning("Too many requests, sleeping for a while...")
            time.sleep(2)
            return self.request(method=method, url=url, **kwargs)
        return response

    @simple_connection_retry
    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def token_has_expired(self, response):
        status = response.status_code
        content_type = response.headers['Content-Type']
        if status == 401 and 'application/json' in content_type:
            return any(
                k in response.json()['description'].lower()
                for k in ['invalid jwt token', 'invalid credentials']
            )
        return False

    def obtain_token(self):
        # if threads are synchronous (for example on start), this should space them out
        time.sleep(random.randint(1, 5))
        if not Variable.get("pnt_token", ""):
            # Obtain new token
            access_token_response = requests.post(
                self.token_url,
                data={'grant_type': 'client_credentials'},
                headers={'Authorization': 'Basic ' + METEO_PNT_APPLICATION_ID},
            )
            token = access_token_response.json()['access_token']
            Variable.set("pnt_token", token)
            # Update session with fresh token
            self.session.headers.update({'Authorization': f'Bearer {token}'})
            logging.info("Fetched and saved new token")
        else:
            logging.info("Checking token in variables")
            token = Variable.get("pnt_token")
            self.session.headers.update({'Authorization': f'Bearer {token}'})
            response = self.session.request("GET", self.test_expiration_url)
            if self.token_has_expired(response):
                logging.warning("Token has expired, fetching a fresh one")
                Variable.set("pnt_token", "")
                self.obtain_token()


def load_issues(current_folder: str) -> list:
    # grib files sometimes have structural issues (cf test_file_structure)
    # if so, we store them in a json file and we'll try to fetch them again
    # and we'll update them if the retrieved versions are not corrupted
    if not os.path.isdir(current_folder):
        os.makedirs(current_folder, )
    issues_file_name = f"{current_folder}/issues.json"
    if not os.path.isfile(issues_file_name):
        with open(issues_file_name, "w") as f:
            json.dump([], f)
        return []
    with open(issues_file_name, "r") as f:
        issues = json.load(f)
    return issues


def save_issues(issues: list, current_folder: str) -> None:
    issues_file_name = f"{current_folder}/issues.json"
    logging.info(f"Dumping issues with {issues}")
    with open(issues_file_name, "w") as f:
        json.dump(issues, f)
