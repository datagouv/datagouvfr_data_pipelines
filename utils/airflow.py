import requests
from pydantic import BaseModel

from airflow.hooks.base import BaseHook
# from airflow.sdk.bases.hook import BaseHook #to use in v3

import airflow_client.client


# What we expect back from auth/token
class AirflowAccessTokenResponse(BaseModel):
    access_token: str


# An optional helper function to retrieve an access token
def get_airflow_client_access_token(
    host: str,
    username: str,
    password: str,
) -> str:
    url = f"{host}/auth/token"
    payload = {
        "username": username,
        "password": password,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 201:
        raise RuntimeError(
            f"Failed to get access token: {response.status_code} {response.text}"
        )
    response_success = AirflowAccessTokenResponse(**response.json())
    return response_success.access_token


class AirflowAPI:
    def __init__(self, conn_name):
        airflow_conn = BaseHook.get_connection(conn_name)

        # Defining the host is optional and defaults to http://localhost
        # See configuration.py for a list of all supported configuration parameters.
        host = airflow_conn.host
        configuration = airflow_client.client.Configuration(host=host)

        # The client must configure the authentication and authorization parameters
        # in accordance with the API server security policy.
        # Examples for each auth method are provided below, use the example that
        # satisfies your auth use case.
        configuration.access_token = get_airflow_client_access_token(
            host=host,
            username=airflow_conn.login,
            password=airflow_conn.password,
        )

        # Instance of the API client to use as a context
        self.client = airflow_client.client.ApiClient(configuration)
