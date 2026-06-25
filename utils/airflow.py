import airflow_client.client
from airflow.sdk.bases.hook import BaseHook
import requests
from pydantic import BaseModel


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
        if not airflow_conn.schema or not airflow_conn.host:
            raise TypeError(f"Schema and host must be set for connection {conn_name}")
        port = f":{str(airflow_conn.port)}" if airflow_conn.port else ""
        self.host = f"{airflow_conn.schema}://{airflow_conn.host}{port}"
        configuration = airflow_client.client.Configuration(host=self.host)
        if not airflow_conn.login or not airflow_conn.password:
            raise TypeError(
                f"Username and password must be set for connection {conn_name}"
            )
        # The client must configure the authentication and authorization parameters
        # in accordance with the API server security policy.
        # Examples for each auth method are provided below, use the example that
        # satisfies your auth use case.
        configuration.access_token = get_airflow_client_access_token(
            host=self.host,
            username=airflow_conn.login,
            password=airflow_conn.password,
        )

        # Instance of the API client to use as a context
        self.client = airflow_client.client.ApiClient(configuration)

    @staticmethod
    def paginate(api_call, result_attr: str, page_size: int = 100, **kwargs):
        """Paginate through all results of an Airflow API list call.

        :param api_call: bound API method to call (e.g. dag_api.get_dags)
        :param result_attr: name of the list attribute on the response object
                            ('dags', 'dag_runs', 'task_instances', ...)
        :param page_size: number of items to fetch per request (default 100 to avoid overload)
        :param kwargs: extra keyword arguments related to the bound API method
                            ('end_date_lte', 'state', ...)
        :return: flat list of all items across pages
        """
        all_items = []
        offset = 0
        while True:
            response = api_call(limit=page_size, offset=offset, **kwargs)
            items = getattr(response, result_attr)
            all_items.extend(items)
            offset += len(items)
            if not items or offset >= response.total_entries:
                break
        return all_items
