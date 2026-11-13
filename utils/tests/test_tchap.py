"""Unit tests for utils.tchap mention rendering.

The conftest stubs ``datagouvfr_data_pipelines.utils.tchap`` so that DAG
notifications remain importable without network access. Here we drop the stub
and load the *real* module to test the mention helpers.
"""

import importlib
import re
import sys


def _load_real_tchap():
    # The conftest stubs config without the TCHAP_* settings the real module
    # imports; add minimal values so the real tchap module can be imported.
    config = sys.modules["datagouvfr_data_pipelines.config"]
    defaults = {
        "TCHAP_BASE_URL": "https://matrix.agent.dinum.tchap.gouv.fr",
        "TCHAP_BOT_TOKEN": "test-token",
        "TCHAP_ROOM_DATAENG": "!prod:agent.dinum.tchap.gouv.fr",
        "TCHAP_ROOM_DATAENG_TEST": "!test:agent.dinum.tchap.gouv.fr",
        "AIRFLOW_ENV": "dev",
    }
    for name, value in defaults.items():
        if not hasattr(config, name):
            setattr(config, name, value)
    sys.modules.pop("datagouvfr_data_pipelines.utils.tchap", None)
    module = importlib.import_module("datagouvfr_data_pipelines.utils.tchap")
    sys.modules["datagouvfr_data_pipelines.utils.tchap"] = module
    return module


tchap = _load_real_tchap()

GEOFFREY_ID = "@geoffrey.aldebert-data.gouv.fr:agent.dinum.tchap.gouv.fr"
GEOFFREY_URL = f"{tchap.TCHAP_WEB_PREFIX}{GEOFFREY_ID}"


def test_user_id():
    assert tchap._user_id("geoffrey") == GEOFFREY_ID
    assert tchap._user_id("unknown") is None


def test_mention_regex_matches_bare_at_name():
    regex = tchap._mention_regex("geoffrey")
    assert regex.search("Ping @Geoffrey")
    assert regex.search("ping @geoffrey")
    # does not match as part of a longer localpart
    assert not regex.search("ping @geoffrey2")
    assert not regex.search("ping @geoffrey.aldebert")
    # does not match an already-built link label
    assert not regex.search("[@geoffrey](https://tchap.gouv.fr/#/@x)")


def test_escape_link_label():
    assert tchap._escape_link_label("Geoffrey Aldebert [Data]") == (
        r"Geoffrey Aldebert \[Data\]"
    )
    assert tchap._escape_link_label("Alice [B") == r"Alice \[B"
    assert tchap._escape_link_label("simple") == "simple"


def test_replace_mentions_in_markdown_uses_display_name(monkeypatch):
    monkeypatch.setattr(
        tchap, "_get_user_display_name", lambda user_id: "Geoffrey Aldebert [Data]"
    )
    result = tchap._replace_mentions_in_markdown(
        "Erreur - ping @geoffrey et @estelle", ["geoffrey"]
    )
    assert r"[Geoffrey Aldebert \[Data\]]({url})".format(url=GEOFFREY_URL) in result
    # the unmentioned user is left untouched
    assert "@estelle" in result
    # the mentioned user no longer appears as a bare @geoffrey
    assert re.search(r"@geoffrey(?![\w.-])", result) is None


def test_replace_mentions_in_markdown_falls_back_to_user_id(monkeypatch):
    monkeypatch.setattr(tchap, "_get_user_display_name", lambda user_id: None)
    result = tchap._replace_mentions_in_markdown("ping @geoffrey", ["geoffrey"])
    assert f"[{GEOFFREY_ID}]({GEOFFREY_URL})" in result


def test_bracket_display_name_renders_as_full_anchor(monkeypatch):
    # a display name with unbalanced brackets must still produce a valid link
    monkeypatch.setattr(
        tchap, "_get_user_display_name", lambda user_id: "Geoffrey Aldebert [Data"
    )
    md = tchap._replace_mentions_in_markdown("Erreur @geoffrey", ["geoffrey"])
    html = tchap.markdown_to_html(md)
    assert f'<a href="{GEOFFREY_URL}">Geoffrey Aldebert [Data</a>' in html


def test_replace_mentions_in_markdown_ignores_unknown_names(monkeypatch):
    monkeypatch.setattr(tchap, "_get_user_display_name", lambda user_id: "X")
    result = tchap._replace_mentions_in_markdown("ping @me", ["me"])
    assert result == "ping @me"


def test_get_user_display_name_success(monkeypatch):
    class FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"displayname": "Geoffrey Aldebert [Data]"}

    captured = {}

    def fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)
    assert tchap._get_user_display_name(GEOFFREY_ID) == "Geoffrey Aldebert [Data]"
    assert captured["url"] == (
        "https://matrix.agent.dinum.tchap.gouv.fr/_matrix/client/v3/profile/"
        + GEOFFREY_ID
    )
    assert captured["headers"]["authorization"] == "Bearer test-token"


def test_get_user_display_name_failure_returns_none(monkeypatch):
    import requests as _requests

    def fake_get(url, headers=None, timeout=None):
        raise _requests.Timeout()

    monkeypatch.setattr("requests.get", fake_get)
    assert tchap._get_user_display_name(GEOFFREY_ID) is None


def test_send_message_builds_pill_and_mentions(monkeypatch):
    captured = {}

    def fake_post(url, headers, json):
        captured["url"] = url
        captured["payload"] = json

        class Resp:
            def raise_for_status(self):
                pass

        return Resp()

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr(
        tchap,
        "_get_user_display_name",
        lambda user_id: "Geoffrey Aldebert [Data]",
    )

    tchap.send_message(
        "Erreur sur le DAG - @geoffrey @estelle",
        room_id="!room:agent.dinum.tchap.gouv.fr",
        ping=["geoffrey", "estelle"],
    )

    payload = captured["payload"]
    # msgtype switched to text because there is a ping
    assert payload["msgtype"] == "m.text"
    # notification via m.mentions
    assert payload["m.mentions"] == {
        "user_ids": [
            "@geoffrey.aldebert-data.gouv.fr:agent.dinum.tchap.gouv.fr",
            "@estelle.bertrand-beta.gouv.fr:agent.dinum.tchap.gouv.fr",
        ]
    }
    # visual pills: markdown links converted to HTML anchors with the web prefix
    assert (
        f'<a href="{GEOFFREY_URL}">Geoffrey Aldebert [Data]</a>'
        in payload["formatted_body"]
    )
    assert "<a href=" in payload["formatted_body"]
    # the bare mention no longer appears in the rendered body
    assert re.search(r"@geoffrey(?![\w.-])", payload["formatted_body"]) is None


def test_send_message_room_mention_keeps_text(monkeypatch):
    captured = {}

    def fake_post(url, headers, json):
        captured["payload"] = json

        class Resp:
            def raise_for_status(self):
                pass

        return Resp()

    monkeypatch.setattr("requests.post", fake_post)

    tchap.send_message(
        "Mise à jour importante",
        room_id="!room:agent.dinum.tchap.gouv.fr",
        ping=["room"],
    )

    payload = captured["payload"]
    assert payload["m.mentions"] == {"room": True}
    assert payload["msgtype"] == "m.text"
