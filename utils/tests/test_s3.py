"""Unit tests for utils.s3 S3Client (real module, mocked boto3 paginator).

The conftest stubs ``datagouvfr_data_pipelines.utils.s3`` so that DAG modules
stay importable without credentials. Here we import the *real* module to test
the paginator logic, then restore the stub so the rest of the test suite is
unaffected.
"""

import importlib
import io
import sys
import types  # noqa: E402
from unittest.mock import MagicMock

import pytest
from urllib3.response import HTTPResponse

S3_MODULE = "datagouvfr_data_pipelines.utils.s3"


def _load_real_s3():
    _airflow_bases = sys.modules.get("airflow.sdk.bases")
    _airflow_bases_hook = sys.modules.get("airflow.sdk.bases.hook")
    original = sys.modules[S3_MODULE]
    try:
        sys.modules["airflow.sdk.bases"] = types.ModuleType("airflow.sdk.bases")
        hook = types.ModuleType("airflow.sdk.bases.hook")
        hook.BaseHook = MagicMock()
        sys.modules["airflow.sdk.bases.hook"] = hook
        sys.modules.pop(S3_MODULE, None)
        module = importlib.import_module(S3_MODULE)
    finally:
        if _airflow_bases is None:
            sys.modules.pop("airflow.sdk.bases", None)
        else:
            sys.modules["airflow.sdk.bases"] = _airflow_bases
        if _airflow_bases_hook is None:
            sys.modules.pop("airflow.sdk.bases.hook", None)
        else:
            sys.modules["airflow.sdk.bases.hook"] = _airflow_bases_hook
        sys.modules[S3_MODULE] = original
    return module


s3 = _load_real_s3()


def _build_client_with_pages(pages):
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    client = MagicMock()
    client.get_paginator.return_value = paginator
    instance = s3.S3Client.__new__(s3.S3Client)
    instance.client = client
    instance.bucket = MagicMock()
    return instance


def test_get_folders_from_prefix_collects_prefixes():
    client = _build_client_with_pages(
        [
            {"CommonPrefixes": [{"Prefix": "2023/"}, {"Prefix": "2024/"}]},
            {"CommonPrefixes": [{"Prefix": "2025/"}]},
        ]
    )
    assert client.get_folders_from_prefix("runs", ignore_airflow_env=True) == [
        "2023/",
        "2024/",
        "2025/",
    ]


def test_get_folders_from_prefix_raises_on_missing_prefix():
    # botocore types mark Prefix optional; real S3 never omits it, but we fail
    # loudly rather than silently returning fewer folders.
    client = _build_client_with_pages(
        [{"CommonPrefixes": [{"Prefix": "2023/"}, {}, {"Prefix": "2024/"}]}]
    )
    with pytest.raises(ValueError, match="Missing 'Prefix'"):
        client.get_folders_from_prefix("runs", ignore_airflow_env=True)


def test_get_folders_from_prefix_prepends_airflow_env():
    client = _build_client_with_pages([{"CommonPrefixes": [{"Prefix": "x/"}]}])
    client.get_folders_from_prefix("runs/", ignore_airflow_env=False)
    client.client.get_paginator.assert_called_once_with("list_objects_v2")
    client.client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket=client.bucket.name,
        Prefix="dev/runs/",
        Delimiter="/",
    )


def test_get_folders_from_prefix_empty_pages():
    client = _build_client_with_pages([{}])
    assert client.get_folders_from_prefix("runs", ignore_airflow_env=True) == []


def _build_client_for_url():
    instance = s3.S3Client.__new__(s3.S3Client)
    instance.client = MagicMock()
    instance.bucket = MagicMock()
    return instance


def _streaming_response(body: bytes) -> HTTPResponse:
    # Real urllib3.HTTPResponse over a BytesIO: exercises the same streaming
    # primitives (read/readinto) that requests' raw exposes.
    return HTTPResponse(
        body=io.BytesIO(body),
        preload_content=False,
        auto_close=False,
        status=200,
    )


class _FakeStreamingSession:
    """Session whose get() returns a context-managed response with .raw."""

    def __init__(self, body: bytes):
        self.body = body
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FakeStreamingResponse(self.body)


class _FakeStreamingResponse:
    def __init__(self, body: bytes):
        self.raw = _streaming_response(body)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.raw.close()
        return False

    def raise_for_status(self):
        return None


def test_send_from_url_streams_full_body_as_buffered_reader():
    body = b"some response payload to stream" * 100
    session = _FakeStreamingSession(body)
    client = _build_client_for_url()
    read_body = []
    # boto3 reads the Fileobj while the HTTP response is still open, so we
    # capture it from within upload_fileobj.
    client.bucket.upload_fileobj.side_effect = lambda **kw: read_body.append(
        kw["Fileobj"].read()
    )

    client.send_from_url("https://varnish.example/x.csv", "x.csv", session=session)

    client.bucket.upload_fileobj.assert_called_once()
    fileobj = client.bucket.upload_fileobj.call_args.kwargs["Fileobj"]
    assert isinstance(fileobj, io.BufferedReader)
    assert read_body == [body]
    assert fileobj.readable()


def test_send_from_url_streams_and_requests_stream_true():
    body = b"payload"
    session = _FakeStreamingSession(body)
    client = _build_client_for_url()

    client.send_from_url("https://varnish.example/x.csv", "dir/x.csv", session=session)

    assert session.calls == [("https://varnish.example/x.csv", {"stream": True})]
    client.bucket.upload_fileobj.assert_called_once()


def test_send_from_url_sets_public_acl_when_requested():
    session = _FakeStreamingSession(b"payload")
    client = _build_client_for_url()

    client.send_from_url(
        "https://varnish.example/x.csv",
        "x.csv",
        session=session,
        is_public=True,
    )

    assert client.bucket.upload_fileobj.call_args[1]["ExtraArgs"] == {
        "ACL": "public-read"
    }


def test_send_from_url_uses_default_requests_when_no_session():
    client = _build_client_for_url()
    original_get = s3.requests.get
    get_mock = MagicMock(return_value=_FakeStreamingResponse(b"payload"))
    s3.requests.get = get_mock
    try:
        client.send_from_url("https://varnish.example/x.csv", "x.csv")
    finally:
        s3.requests.get = original_get

    get_mock.assert_called_once_with("https://varnish.example/x.csv", stream=True)
    client.bucket.upload_fileobj.assert_called_once()
