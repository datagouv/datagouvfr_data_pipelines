"""Pytest configuration for the datagouvfr_data_pipelines.utils tests.

Makes the ``datagouvfr_data_pipelines`` namespace package importable by adding
its parent directory to ``sys.path`` (the repo uses namespace packages without
``__init__.py``, and the package parent is not on the path by default).
"""

import sys
from pathlib import Path

# <repo>/datagouvfr_data_pipelines/utils/tests -> <repo>/dags parent on path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
