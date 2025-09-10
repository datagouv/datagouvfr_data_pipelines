import os
import requests

DEMO_DATAGOUV_SECRET_API_KEY = os.getenv("DEMO_DATAGOUV_SECRET_API_KEY")


def delete_demo_topic(topic_id: str):
    response = requests.delete(
        f"https://demo.data.gouv.fr/api/2/topics/{topic_id}/",
        headers={"X-API-KEY": DEMO_DATAGOUV_SECRET_API_KEY},
    )
    if response.status_code != 204:
        print(response.status_code, response.json())


def get_demo_topics(tags: str = None):
    response = requests.get(
        "https://demo.data.gouv.fr/api/2/topics/",
        params={"tag": tags, "page_size": 1000, "page": 1, "include_private": True},
        headers={"X-API-KEY": DEMO_DATAGOUV_SECRET_API_KEY},
    )
    return response


response = get_demo_topics("simplifions")

if response.status_code != 200:
    print(response.status_code, response.json())
    exit(1)

response_json = response.json()
simplifions_topics = response_json["data"]

print(f"Found {len(simplifions_topics)} topics out of {response_json['total']}")

for topic in simplifions_topics:
    print(".", end="", flush=True)
    delete_demo_topic(topic["id"])

print("Done")
