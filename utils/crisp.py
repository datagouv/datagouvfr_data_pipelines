import logging

import requests
from datagouvfr_data_pipelines.config import (
    CRISP_PLUGIN_ID,
    CRISP_PLUGIN_TOKEN,
    CRISP_USER_ID,
    CRISP_USER_TOKEN,
    CRISP_WEBSITE_ID,
)
from requests.auth import HTTPBasicAuth

# the website id is retrieved from the main crisp app: Settings > Workspace settings > Integrations
# the plugin id and token are retrieved from https://marketplace.crisp.chat/plugins/
# the user id and token are retrieved from the main crisp app:
# open the browser console and type "localStorage.user_session"

base_url = f"https://api.crisp.chat/v1/website/{CRISP_WEBSITE_ID}/conversations/"


def get_page_conversations(
    page: int,
    url: str,
    tier: str,
    auth: HTTPBasicAuth,
) -> list[dict]:
    res = requests.get(
        url + str(page),
        headers={
            "Content-Type": "application/json",
            "X-Crisp-Tier": tier,
        },
        auth=auth,
    )
    res.raise_for_status()
    return res.json()["data"]


def get_all_conversations() -> list[dict]:
    logging.info("Getting normal tickets")
    shared_kwargs = {
        "url": base_url,
        "tier": "plugin",
        "auth": requests.auth.HTTPBasicAuth(CRISP_PLUGIN_ID, CRISP_PLUGIN_TOKEN),
    }
    page = 1
    convs = []
    this_page_convs = get_page_conversations(page=page, **shared_kwargs)
    while this_page_convs:
        convs += this_page_convs
        logging.info(f"> Got page {page}")
        page += 1
        this_page_convs = get_page_conversations(page=page, **shared_kwargs)
    return convs


def get_all_spam_conversations() -> list[dict]:
    # the /spams endpoint is not available with plugin credentials
    logging.info("Getting spam tickets")
    shared_kwargs = {
        "url": base_url + "spams/",
        "tier": "user",
        "auth": requests.auth.HTTPBasicAuth(CRISP_USER_ID, CRISP_USER_TOKEN),
    }
    page = 1
    convs = []
    this_page_convs = get_page_conversations(page=page, **shared_kwargs)
    while this_page_convs:
        convs += this_page_convs
        print(f"> Got page {page}")
        page += 1
        this_page_convs = get_page_conversations(page=page, **shared_kwargs)
    return convs
