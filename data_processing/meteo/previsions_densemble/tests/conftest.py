"""Pytest configuration for the meteo previsions_densemble tests.

Stubs out Airflow, the data.gouv.fr config and the utils modules so that
``task_functions`` can be imported and unit-tested without an Airflow runtime
or real S3/SFTP access.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

repo_root = Path(__file__).parent.parent.parent.parent.parent

previsions_densemble_dir = (
    repo_root / "data_processing" / "meteo" / "previsions_densemble"
)

# Make the module importable directly as 'task_functions'
sys.path.insert(0, str(previsions_densemble_dir))


def _stub_package(name, path=None, **attrs):
    """Register a fake package in sys.modules."""
    module = types.ModuleType(name)
    if path is not None:
        module.__path__ = [str(path)]
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


# --- Airflow ---
def _task_passthrough(fn=None, **kwargs):
    if fn is not None:
        return fn

    def decorator(f):
        return f

    return decorator


_stub_package("airflow")
_stub_package("airflow.sdk", task=_task_passthrough)

# --- datagouvfr_data_pipelines package tree ---
packages = [
    ("datagouvfr_data_pipelines", repo_root),
    ("datagouvfr_data_pipelines.data_processing", None),
    ("datagouvfr_data_pipelines.data_processing.meteo", None),
    # 'previsions_densemble' points at the real directory so task_functions is importable
    (
        "datagouvfr_data_pipelines.data_processing.meteo.previsions_densemble",
        previsions_densemble_dir,
    ),
    ("datagouvfr_data_pipelines.utils", repo_root / "utils"),
]
for name, path in packages:
    _stub_package(name, path)

# --- config mock ---
config_mock = MagicMock()
config_mock.AIRFLOW_ENV = "dev"
config_mock.AIRFLOW_DAG_TMP = "/tmp/"
# so that task_functions can load its config.json at import time
config_mock.AIRFLOW_DAG_HOME = str(repo_root.parent) + "/"
config_mock.DATAGOUV_SECRET_API_KEY = "test-key"
config_mock.DEMO_DATAGOUV_SECRET_API_KEY = "test-demo-key"
sys.modules["datagouvfr_data_pipelines.config"] = config_mock

# --- utils modules imported by task_functions ---
_stub_package(
    "datagouvfr_data_pipelines.utils.datagouv",
    local_client=MagicMock(),
)
_stub_package(
    "datagouvfr_data_pipelines.utils.filesystem",
    File=MagicMock(),
)
_stub_package(
    "datagouvfr_data_pipelines.utils.s3",
    S3Client=MagicMock(),
    S3ClientKwargs=dict,
)
_stub_package(
    "datagouvfr_data_pipelines.utils.sftp",
    SFTPClient=MagicMock(),
)
