import logging
import re

import requests
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    TCHAP_BASE_URL,
    TCHAP_BOT_TOKEN,
    TCHAP_ROOM_DATAENG,
    TCHAP_ROOM_DATAENG_TEST,
)
from markdown import markdown

# https://spec.matrix.org/unstable/client-server-api/#size-limits
MAX_MESSAGE_LENGTH = 65536

TCHAP_SERVER = "agent.dinum.tchap.gouv.fr"
# Web client permalink prefix used for the mention pill href (matches what the
# Tchap web client produces when a user selects a mention).
TCHAP_WEB_PREFIX = "https://tchap.gouv.fr/#/"

map_ping = {
    "geoffrey": "geoffrey.aldebert-data.gouv.fr",
    "hadrien": "hadrien.bossard.ext-mail.numerique.gouv.fr",
    "estelle": "estelle.bertrand-beta.gouv.fr",
    "valentin": "valentin.shamsnejad-beta.gouv.fr",
    "alexandre": "alexandre.bulte.ext-numerique.gouv.fr",
    "ludine": "ludine.pierquin.ext-mail.numerique.gouv.fr",
    "antonin": "antonin.garrone-data.gouv.fr",
}


def markdown_to_html(md: str) -> str:
    return markdown(md).replace("\n", "")


def _user_id(name: str) -> str | None:
    """Full Matrix user id for a ``map_ping`` key, or None if unknown."""
    localpart = map_ping.get(name)
    if localpart is None:
        return None
    return f"@{localpart}:{TCHAP_SERVER}"


def _mention_regex(name: str) -> re.Pattern[str]:
    """Match a bare ``@name`` in markdown, skipping already-built links.

    Rejects ``@name`` that is part of a longer localpart (``@name.xxx``,
    ``@name-xxx``) or that already follows a ``[`` (start of a link label).
    """
    return re.compile(
        rf"(?<![\w@[\-])@({re.escape(name)})(?![\w.-])",
        flags=re.IGNORECASE,
    )


def _get_user_display_name(user_id: str) -> str | None:
    """Fetch the human display name of a Tchap user (e.g. ``Geoffrey Aldebert [Data]``).

    Uses the same homeserver profile API as the Tchap web client. Returns
    ``None`` on any failure so the caller can fall back to the user id.
    """
    if not (TCHAP_BASE_URL and TCHAP_BOT_TOKEN):
        return None
    url = f"{TCHAP_BASE_URL}/_matrix/client/v3/profile/{user_id}"
    try:
        r = requests.get(
            url,
            headers={"authorization": f"Bearer {TCHAP_BOT_TOKEN}"},
            timeout=10,
        )
        r.raise_for_status()
        displayname = r.json().get("displayname")
        return displayname if displayname else None
    except (requests.RequestException, ValueError, KeyError):
        logging.warning(f"Could not fetch Tchap display name for {user_id}")
        return None


def _escape_link_label(text: str) -> str:
    """Escape ``[``/``]`` in a markdown link label so we stay in control of the
    rendering regardless of what the display name contains (the display name
    comes from the Tchap profile API and is out of our control)."""
    return text.replace("[", r"\[").replace("]", r"\]")


def _replace_mentions_in_markdown(text: str, names: list[str]) -> str:
    """Turn ``@name`` mentions in the markdown into Tchap mention links.

    The display text is the human name fetched from the Tchap profile API,
    falling back to the full user id. ``m.mentions`` (set separately) is what
    triggers the notification; the link is only the visual, clickable pill.
    """
    for name in names:
        user_id = _user_id(name)
        if user_id is None:
            continue
        display = _get_user_display_name(user_id) or user_id
        link = f"[{_escape_link_label(display)}]({TCHAP_WEB_PREFIX}{user_id})"
        text = _mention_regex(name).sub(lambda _m: link, text)
    return text


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
        # render the mentioned users as clickable pills in the markdown text.
        # unknown names (e.g. a "room" ping) are skipped by the replacement.
        text = _replace_mentions_in_markdown(text, ping)
    formatted = markdown_to_html(text)
    if formatted.__sizeof__() > MAX_MESSAGE_LENGTH:
        logging.warning(
            f"The message is too long ({formatted.__sizeof__()} bytes), truncating..."
        )
        formatted = formatted[:MAX_MESSAGE_LENGTH]
        while formatted.__sizeof__() > MAX_MESSAGE_LENGTH:
            formatted = formatted[:-10]

    payload: dict = {
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
                    f"@{map_ping[name]}:{TCHAP_SERVER}"
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
