import logging
import time
import random
from typing import List, Dict, Union
from requests.exceptions import SSLError
import requests

RNE_API_TOKEN_URL = "https://registre-national-entreprises.inpi.fr/api/sso/login"
RNE_API_DIFF_URL = "https://registre-national-entreprises.inpi.fr/api/companies/diff?"


def create_persistent_session():
    """Create a session with a custom HTTP adapter for max retries."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=20)
    session.mount("http://", adapter)

    return session


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
                    time.sleep(600)
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


def get_last_siren_in_page(page_data):
    return page_data[-1].get("company", {}).get("siren") if page_data else None
