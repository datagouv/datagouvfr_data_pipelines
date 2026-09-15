"""Pytest configuration for the DVF explore tests.

Stubs out Airflow, the data.gouv.fr config and the utils modules so that
``task_functions`` can be imported and unit-tested without an Airflow runtime
or real S3/API access.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

repo_root = Path(__file__).parent.parent.parent.parent.parent

explore_dir = repo_root / "data_processing" / "dvf" / "explore"

# Make the repo root importable as 'datagouvfr_data_pipelines'
sys.path.insert(0, str(repo_root))


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
    ("datagouvfr_data_pipelines.data_processing.dvf", None),
    # 'explore' points at the real directory so task_functions is importable
    ("datagouvfr_data_pipelines.data_processing.dvf.explore", explore_dir),
    ("datagouvfr_data_pipelines.utils", None),
]
for name, path in packages:
    _stub_package(name, path)

# --- config mock ---
sys.modules["datagouvfr_data_pipelines.config"] = MagicMock()

# --- utils modules imported by task_functions ---
_stub_package(
    "datagouvfr_data_pipelines.utils.conversions",
    csv_to_csvgz=MagicMock(),
    csv_to_geoparquet=MagicMock(),
)
_stub_package("datagouvfr_data_pipelines.utils.datagouv", local_client=MagicMock())
_stub_package("datagouvfr_data_pipelines.utils.filesystem", File=MagicMock())
_stub_package("datagouvfr_data_pipelines.utils.postgres", PostgresClient=MagicMock())
_stub_package("datagouvfr_data_pipelines.utils.s3", S3Client=MagicMock())
_stub_package("datagouvfr_data_pipelines.utils.tchap", send_message=MagicMock())
