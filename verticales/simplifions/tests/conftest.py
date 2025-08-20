"""
Simple configuration for pytest: treat this repo root as 'datagouvfr_data_pipelines'
"""

import sys
import types
from pathlib import Path

repo_root = Path(__file__).parent.parent.parent.parent

# Add simplifions directory to sys.path to allow imports from the simplifions directory in its tests
simplifions_dir = repo_root / "verticales" / "simplifions"
sys.path.insert(0, str(simplifions_dir))

# Create all necessary packages to satisfy the datagouvfr_data_pipelines imports
packages = [
    ("datagouvfr_data_pipelines", repo_root),
    ("datagouvfr_data_pipelines.utils", repo_root / "utils"),
]

for package_name, package_path in packages:
    module = types.ModuleType(package_name)
    module.__path__ = [str(package_path)]
    sys.modules[package_name] = module
