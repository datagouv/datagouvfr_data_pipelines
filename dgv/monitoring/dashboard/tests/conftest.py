"""Pytest configuration for the dashboard preview-stats tests.

Stubs out Airflow, the data.gouv.fr config, and the utils/s3/filesystem
modules so that ``preview_stats`` can be imported and unit-tested without
an Airflow runtime or real S3/API access.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

repo_root = Path(__file__).parent.parent.parent.parent.parent

dashboard_dir = repo_root / "dgv" / "monitoring" / "dashboard"

# Make the repo root importable as 'datagouvfr_data_pipelines'
sys.path.insert(0, str(repo_root))


def _stub_package(name, path=None):
    """Register a fake top-level package in sys.modules."""
    module = types.ModuleType(name)
    if path is not None:
        module.__path__ = [str(path)]
    sys.modules[name] = module
    return module


# --- Airflow ---
airflow = types.ModuleType("airflow")
airflow_sdk = types.ModuleType("airflow.sdk")


def _task_passthrough(fn=None, **kwargs):
    if fn is not None:
        return fn

    def decorator(f):
        return f

    return decorator


airflow_sdk.task = _task_passthrough  # type: ignore[attr-defined]
sys.modules["airflow"] = airflow
sys.modules["airflow.sdk"] = airflow_sdk


# --- datagouvfr_data_pipelines package tree (stub empty packages) ---
packages = [
    ("datagouvfr_data_pipelines", repo_root),
    ("datagouvfr_data_pipelines.dgv", None),
    ("datagouvfr_data_pipelines.dgv.monitoring", None),
    # 'dashboard' points at the real directory so preview_stats is importable
    ("datagouvfr_data_pipelines.dgv.monitoring.dashboard", dashboard_dir),
    ("datagouvfr_data_pipelines.utils", None),
    ("datagouvfr_data_pipelines.utils.crisp", None),
    ("datagouvfr_data_pipelines.utils.datagouv", None),
    ("datagouvfr_data_pipelines.utils.filesystem", None),
    ("datagouvfr_data_pipelines.utils.s3", None),
    ("datagouvfr_data_pipelines.utils.utils", None),
]
for name, path in packages:
    _stub_package(name, path)

# --- config mock ---
config = MagicMock()
config.AIRFLOW_DAG_TMP = "/tmp/"
sys.modules["datagouvfr_data_pipelines.config"] = config

# --- utils deps imported by the real task_functions module ---
crisp = sys.modules["datagouvfr_data_pipelines.utils.crisp"]
crisp.get_all_conversations = MagicMock()  # type: ignore[attr-defined]
crisp.get_all_spam_conversations = MagicMock()  # type: ignore[attr-defined]

datagouv = sys.modules["datagouvfr_data_pipelines.utils.datagouv"]
datagouv.DATAGOUV_MATOMO_ID = "id"  # type: ignore[attr-defined]
datagouv.local_client = MagicMock()  # type: ignore[attr-defined]

utils = sys.modules["datagouvfr_data_pipelines.utils.utils"]
utils.list_months_between = MagicMock()  # type: ignore[attr-defined]

# --- utils.filesystem.File ---
filesystem = sys.modules["datagouvfr_data_pipelines.utils.filesystem"]
filesystem.File = MagicMock()  # type: ignore[attr-defined]

# --- utils.s3.S3Client ---
s3 = sys.modules["datagouvfr_data_pipelines.utils.s3"]
s3.S3Client = MagicMock()  # type: ignore[attr-defined]
s3.S3ClientKwargs = MagicMock()  # type: ignore[attr-defined]

# --- task_functions (only DAG_NAME is imported by preview_stats) ---
task_functions = _stub_package(
    "datagouvfr_data_pipelines.dgv.monitoring.dashboard.task_functions"
)
task_functions.DAG_NAME = "dgv_dashboard"
