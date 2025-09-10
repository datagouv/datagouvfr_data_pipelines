import requests
import logging
from datagouv import Client
from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
)


class TopicsAPI:
    def __init__(self, client: Client):
        self.client = client
        # We need to provide the api key in the headers of our requests
        # because the client doesn't have built-in api key management
        # for the topics endpoints for now
        self.dgv_headers = client.session.headers
        self.resource_url = f"{self.client.base_url}/api/2/topics/"

    def _topic_url(self, topic_id: str) -> str:
        return f"{self.resource_url}/{topic_id}/"

    def create_topic(self, topic_data: dict):
        url = self.resource_url
        r = requests.post(
            url,
            headers=self.dgv_headers,
            json=topic_data,
        )
        r.raise_for_status()
        logging.info(f"Created topic {topic_data['name']}")
        return r

    def delete_topic(self, topic_id: str):
        url = self._topic_url(topic_id)
        r = requests.delete(
            url,
            headers=self.dgv_headers,
        )
        r.raise_for_status()
        logging.info(f"Deleted topic at {url}")
        return r

    def update_topic_by_id(self, topic_id: str, topic_data: dict):
        url = self._topic_url(topic_id)
        r = requests.put(
            url,
            headers=self.dgv_headers,
            json=topic_data,
        )
        r.raise_for_status()
        logging.info(f"Updated topic at {url}")
        return r

    def get_all_topics_for_tag(self, tag: str) -> list[dict]:
        return get_all_from_api_query(
            f"{self.resource_url}?tag={tag}&include_private=true",
            auth=True,
        )
