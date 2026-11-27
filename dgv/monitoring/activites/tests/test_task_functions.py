"""Unit tests for the structured publication detection.

Datasets published through the structured publication flow carry
``extras["publish_source"] == "data.gouv.fr:structured"`` at the dataset level
(no resource-level extra). ``parse_schema_catalog`` reports it and
``format_schema_resource_message`` appends a "Made with the structured
publication" badge to the Tchap message.
"""

import requests
from datagouvfr_data_pipelines.dgv.monitoring.activites.task_functions import (
    format_schema_resource_message,
    parse_resource_if_schema,
    parse_schema_catalog,
)

SCHEMA_NAME = "etalab/schema-example"


def _catalog():
    return [
        {
            "name": SCHEMA_NAME,
            "title": "Schema Example",
            "schema_type": "tableschema",
            "schema_url": f"https://schema.data.gouv.fr/{SCHEMA_NAME}.json",
        }
    ]


def _resource():
    return {
        "name": "data.csv",
        "url": "https://www.data.gouv.fr/fr/datasets/example/data.csv",
        "schema": {"name": SCHEMA_NAME},
        "extras": {},
    }


def _dataset(publish_source=None):
    dataset = {"page": "https://www.data.gouv.fr/fr/datasets/example/"}
    if publish_source is not None:
        dataset["extras"] = {"publish_source": publish_source}
    return dataset


class _ValidResponse:
    def json(self):
        return {"report": {"valid": True}}


def _valid_get(url, *args, **kwargs):
    return _ValidResponse()


def test_parse_schema_catalog_detects_structured_publication(monkeypatch):
    monkeypatch.setattr(requests, "get", _valid_get)
    structured = parse_schema_catalog(_catalog(), _resource(), "data.gouv.fr:structured")[1]
    assert structured is True


def test_parse_schema_catalog_ignores_other_publish_sources(monkeypatch):
    monkeypatch.setattr(requests, "get", _valid_get)
    for publish_source in ("data.gouv.fr:new", "publier.etalab.studio", ""):
        structured = parse_schema_catalog(_catalog(), _resource(), publish_source)[1]
        assert structured is False


def test_parse_schema_catalog_returns_none_for_unknown_schema(monkeypatch):
    monkeypatch.setattr(requests, "get", _valid_get)
    resource = _resource()
    resource["schema"] = {"name": "unknown/schema"}
    assert parse_schema_catalog(_catalog(), resource, "data.gouv.fr:structured") == (
        None,
        None,
        None,
        None,
        None,
    )


def test_format_message_adds_structured_badge():
    message = format_schema_resource_message(
        "Schema Example",
        "(Organisation Test)",
        _dataset("data.gouv.fr:structured"),
        "tableschema",
        "https://validata.fr/table-schema?input=url",
        True,
        True,
    )
    assert "Made with the structured publication" in message


def test_format_message_no_badge_for_classic_publication():
    message = format_schema_resource_message(
        "Schema Example",
        "",
        _dataset("data.gouv.fr:new"),
        "tableschema",
        "https://validata.fr/table-schema?input=url",
        True,
        False,
    )
    assert "Made with" not in message


def test_parse_resource_if_schema_sends_structured_badge(monkeypatch):
    monkeypatch.setattr(requests, "get", _valid_get)
    messages = []
    monkeypatch.setattr(
        "datagouvfr_data_pipelines.dgv.monitoring.activites.task_functions.send_message",
        lambda text, room: messages.append(text),
    )
    assert parse_resource_if_schema(
        _catalog(), _resource(), _dataset("data.gouv.fr:structured"), "(Organisation Test)"
    )
    assert "Made with the structured publication" in messages[0]


def test_parse_resource_if_schema_no_badge_for_new_dataset(monkeypatch):
    monkeypatch.setattr(requests, "get", _valid_get)
    messages = []
    monkeypatch.setattr(
        "datagouvfr_data_pipelines.dgv.monitoring.activites.task_functions.send_message",
        lambda text, room: messages.append(text),
    )
    assert parse_resource_if_schema(
        _catalog(), _resource(), _dataset("data.gouv.fr:new"), "(Organisation Test)"
    )
    assert "Made with" not in messages[0]
    assert "[Ressource valide]" in messages[0]
