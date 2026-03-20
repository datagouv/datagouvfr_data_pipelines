import logging

import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    TCHAP_DATAGOUV_WEBHOOK,
    TCHAP_ROOM_DATAENG,
    TCHAP_ROOM_DATAENG_TEST,
)


def send_message(
    text: str,
    endpoint_url: str | None = None,
) -> None:
    """Send a message to a Tchap channel.

    Args:
        endpoint_url (str): URL of the Tchap endpoint (for bot)
        text (str): Text to send to a channel
        image_url (Optional[str], optional): Url of an image to link
        with your text. Defaults to None.
    """
    if not endpoint_url:
        endpoint_url = (
            TCHAP_ROOM_DATAENG
            if AIRFLOW_ENV == "prod"
            else TCHAP_ROOM_DATAENG_TEST
        )
    data = {"roomId": TCHAP_ANNUAIRE_ROOM_ID, "message": text}

    try:
        response = requests.post(endpoint_url, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send message: {e}")
        raise Exception(f"Failed to send message: {e}")