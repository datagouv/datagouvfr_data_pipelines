import logging

from markdown import markdown
import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    TCHAP_BASE_URL,
    TCHAP_BOT_TOKEN,
    TCHAP_ROOM_DATAENG,
    TCHAP_ROOM_DATAENG_TEST,
)

# https://spec.matrix.org/unstable/client-server-api/#size-limits
MAX_MESSAGE_LENGTH = 65536

map_ping = {
    "geoffrey": "geoffrey.aldebert-data.gouv.fr",
    "hadrien": "hadrien.bossard.ext-mail.numerique.gouv.fr",
    "pierlou": "pierlou.ramade-data.gouv.fr",
    "valentin": "valentin.shamsnejad-beta.gouv.fr",
    "alexandre": "alexandre.bulte.ext-numerique.gouv.fr",
    "ludine": "ludine.pierquin.ext-mail.numerique.gouv.fr",
    "antonin": "antonin.garrone-data.gouv.fr",
}


def markdown_to_html(md: str) -> str:
    return markdown(md).replace("\n", "")


def send_message(
    text: str,
    room_id: str = (
        TCHAP_ROOM_DATAENG if AIRFLOW_ENV == "prod" else TCHAP_ROOM_DATAENG_TEST
    ),
    *,
    message_type: str = "notice",  # whether to send a real message or an insight
    ping: list[str] = [],
) -> None:
    """Send a message to a Tchap channel."""
    if not (TCHAP_BASE_URL and TCHAP_BOT_TOKEN):
        raise ValueError("Tchap settings (URL and token) are not set")
    if not room_id:
        if AIRFLOW_ENV == "prod":
            raise ValueError("The room id is not set")
        else:
            room_id = TCHAP_ROOM_DATAENG_TEST
    assert message_type in {"text", "notice"}
    if ping:
        # notice doesn't ping
        message_type = "text"
    formatted = markdown_to_html(text)
    if formatted.__sizeof__() > MAX_MESSAGE_LENGTH:
        logging.warning(
            f"The message is too long ({formatted.__sizeof__()} bytes), truncating..."
        )
        formatted = formatted[:MAX_MESSAGE_LENGTH]
        while formatted.__sizeof__() > MAX_MESSAGE_LENGTH:
            formatted = formatted[:-10]

    payload = {
        "msgtype": f"m.{message_type}",
        "body": "_",
        "format": "org.matrix.custom.html",
        "formatted_body": formatted,
    }
    if ping:
        if ping[0] == "room":
            payload["m.mentions"] = {"room": True}
        else:
            payload["m.mentions"] = {
                "user_ids": [
                    f"@{map_ping[name]}:agent.dinum.tchap.gouv.fr"
                    for name in ping
                    if name in map_ping
                ],
            }
    r = requests.post(
        f"{TCHAP_BASE_URL}/_matrix/client/v3/rooms/{room_id}/send/m.room.message",
        headers={
            "content-type": "application/json",
            "authorization": f"Bearer {TCHAP_BOT_TOKEN}",
        },
        json=payload,
    )
    r.raise_for_status()
