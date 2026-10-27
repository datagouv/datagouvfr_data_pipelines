"""Shared pytest setup for the whole repository.

Tests import DAG code as ``datagouvfr_data_pipelines.<...>``. The pytest
``pythonpath`` setting (see ``pytest.ini``) puts the repository parent on
``sys.path``, so the namespace package resolves without per-suite hacks.

External dependencies and side-effecting modules are stubbed once here so that
unit tests can import DAG modules without an Airflow runtime, network access or
credentials. Modules under test (``utils.format``, ``utils.utils``, ...) are
left untouched.
"""

import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

repo_root = Path(__file__).resolve().parent


def _stub_module(name: str, **attrs) -> types.ModuleType:
    """Register a fake module in sys.modules."""
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


def _task_passthrough(fn=None, **kwargs):
    if fn is not None:
        return fn

    def decorator(f):
        return f

    return decorator


# --- Airflow runtime ---
_stub_module("airflow")
_stub_module("airflow.sdk", task=_task_passthrough)
_stub_module("airflow.sdk.definitions")

# --- datagouvfr_data_pipelines.config ---
_stub_module(
    "datagouvfr_data_pipelines.config",
    AIRFLOW_DAG_HOME=f"{repo_root.parent}/",
    AIRFLOW_DAG_TMP="/tmp/",
    AIRFLOW_ENV="dev",
    DATAGOUV_SECRET_API_KEY="test-key",
    DEMO_DATAGOUV_SECRET_API_KEY="test-demo-key",
    S3_BUCKET_DATA_PIPELINE="test-bucket",
    S3_BUCKET_DATA_PIPELINE_OPEN="test-open-bucket",
)

# --- datagouv client library ---
_stub_module("datagouv", Dataset=MagicMock(), Resource=MagicMock)

# --- utils modules with side effects or external dependencies ---
_stub_module(
    "datagouvfr_data_pipelines.utils.conversions",
    csv_to_csvgz=MagicMock(),
    csv_to_geoparquet=MagicMock(),
    csv_to_parquet=MagicMock(),
)
_stub_module(
    "datagouvfr_data_pipelines.utils.crisp",
    get_all_conversations=MagicMock(),
    get_all_spam_conversations=MagicMock(),
)
_stub_module(
    "datagouvfr_data_pipelines.utils.datagouv",
    DATAGOUV_MATOMO_ID="test-matomo-id",
    demo_client=MagicMock(),
    local_client=MagicMock(),
    prod_client=MagicMock(),
)
_stub_module("datagouvfr_data_pipelines.utils.postgres", PostgresClient=MagicMock())
_stub_module(
    "datagouvfr_data_pipelines.utils.retry", simple_connection_retry=lambda f: f
)
_stub_module("datagouvfr_data_pipelines.utils.download", download_files=MagicMock())
# Keep the real filesystem helpers (tested by the metrics suite) but avoid
# instantiating File, which probes the filesystem and calls libmagic.
_filesystem = importlib.import_module("datagouvfr_data_pipelines.utils.filesystem")
_filesystem.File = MagicMock()
_stub_module(
    "datagouvfr_data_pipelines.utils.s3",
    S3Client=MagicMock(),
    S3ClientKwargs=dict,
)
_stub_module("datagouvfr_data_pipelines.utils.sftp", SFTPClient=MagicMock())
_stub_module("datagouvfr_data_pipelines.utils.tchap", send_message=MagicMock())

# --- Heavy geoloc helpers, mocked to keep task_functions importable ---
_geoloc_utils = "datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils"
_stub_module(f"{_geoloc_utils}.cadastre_index", enrich_with_cadastre=MagicMock())
_stub_module(f"{_geoloc_utils}.yearly_enrich", enrich_year=MagicMock())
