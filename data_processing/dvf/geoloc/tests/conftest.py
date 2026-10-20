"""Pytest configuration for the DVF geoloc tests.

Stubs out Airflow, the data.gouv.fr config and the utils modules so that
``task_functions`` can be imported and unit-tested without an Airflow runtime
or real S3/API access.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

repo_root = Path(__file__).parent.parent.parent.parent.parent

geoloc_dir = repo_root / "data_processing" / "dvf" / "geoloc"

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


def _condition_passthrough(condition=None, skip_message=None):
    """Passthrough for @run_if / @skip_if: the task is returned unchanged.

    (The decorators wrap a _TaskDecorator's pre_execute hook; in tests we don't
    emulate the Airflow run machinery, so conditions are no-ops.)
    """

    def decorator(task):
        return task

    return decorator


_stub_package("airflow")
_stub_package("airflow.sdk", task=_task_passthrough)
# airflow.sdk defines ``airflow.sdk.definitions.decorators`` as a sub-package;
# it must be stubbed too since ``task`` and ``run_if``/``skip_if`` live there
# (the latter are imported from the ``condition`` submodule).
_decorators = types.ModuleType("airflow.sdk.definitions.decorators")
_airflow_def = types.ModuleType("airflow.sdk.definitions")
_airflow_def.__path__ = []
_condition = types.ModuleType("airflow.sdk.definitions.decorators.condition")
# Use setattr so that mypy (which special-cases ModuleType) stays quiet.
setattr(_decorators, "task", _task_passthrough)
setattr(_decorators, "run_if", _condition_passthrough)
setattr(_decorators, "skip_if", _condition_passthrough)
setattr(_decorators, "condition", _condition)
setattr(_airflow_def, "decorators", _decorators)
setattr(_condition, "run_if", _condition_passthrough)
setattr(_condition, "skip_if", _condition_passthrough)
sys.modules["airflow.sdk.definitions"] = _airflow_def
sys.modules["airflow.sdk.definitions.decorators"] = _decorators
sys.modules["airflow.sdk.definitions.decorators.condition"] = _condition

# --- datagouvfr_data_pipelines package tree ---
packages = [
    ("datagouvfr_data_pipelines", repo_root),
    ("datagouvfr_data_pipelines.data_processing", None),
    ("datagouvfr_data_pipelines.data_processing.dvf", None),
    ("datagouvfr_data_pipelines.data_processing.dvf.geoloc", geoloc_dir),
    ("datagouvfr_data_pipelines.utils", None),
]
for name, path in packages:
    _stub_package(name, path)

# --- config mock ---
config_module = types.ModuleType("datagouvfr_data_pipelines.config")
for key in ("AIRFLOW_DAG_HOME", "AIRFLOW_DAG_TMP", "AIRFLOW_ENV"):
    setattr(config_module, key, "")
sys.modules["datagouvfr_data_pipelines.config"] = config_module

# --- datagouv library mock ---
datagouv_module = types.ModuleType("datagouv")
setattr(datagouv_module, "Dataset", MagicMock())
setattr(datagouv_module, "Resource", MagicMock)
sys.modules["datagouv"] = datagouv_module

# --- utils modules imported by task_functions ---
_stub_package(
    "datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils",
    geoloc_dir / "utils",
)
_stub_package(
    "datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.cadastre_index",
    enrich_with_cadastre=MagicMock(),
)
_stub_package(
    "datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.yearly_enrich",
    enrich_year=MagicMock(),
)
_stub_package(
    "datagouvfr_data_pipelines.utils.datagouv",
    local_client=MagicMock(),
    demo_client=MagicMock(),
)
_stub_package(
    "datagouvfr_data_pipelines.utils.retry", simple_connection_retry=lambda f: f
)
_stub_package("datagouvfr_data_pipelines.utils.s3", S3Client=MagicMock())
_stub_package("datagouvfr_data_pipelines.utils.tchap", send_message=MagicMock())
