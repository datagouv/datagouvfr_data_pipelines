import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    TCHAP_BASE_URL,
    TCHAP_BOT_TOKEN,
    TCHAP_ROOM_DATAENG,
    TCHAP_ROOM_DATAENG_TEST,
)


def send_message(
    text: str,
    room_id: str | None = None,
) -> None:
    """Send a message to a Tchap channel."""
    room_id = room_id or (
        TCHAP_ROOM_DATAENG
        if AIRFLOW_ENV == "prod"
        else TCHAP_ROOM_DATAENG_TEST
    )
    r = requests.post(
        f"{TCHAP_BASE_URL}/_matrix/client/v3/rooms/{room_id}/send/m.room.message?access_token={TCHAP_BOT_TOKEN}",
        headers={"content-type": "application/json"},
        json={
          "msgtype": "m.text",
          "body": "_",
          "format": "org.matrix.custom.html",
          "formatted_body": text,
        },
    )
    r.raise_for_status()
