"""
Simple configuration for pytest: treat this repo root as 'datagouvfr_data_pipelines'
"""

import sys
import types
from pathlib import Path
from unittest.mock import Mock, patch

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


# Set up config mock that will be available for all tests
config_mock = Mock()
# Datagouvfr config
config_mock.AIRFLOW_ENV = "dev"
config_mock.DATAGOUV_SECRET_API_KEY = "test-key"
config_mock.DEMO_DATAGOUV_SECRET_API_KEY = "test-demo-key"
# Grist config
config_mock.GRIST_API_URL = "https://grist.example.com/api/"
config_mock.SECRET_GRIST_API_KEY = "test-api-key"
# MinIO config
config_mock.MINIO_URL = "minio.example.com"
config_mock.SECRET_MINIO_DATA_PIPELINE_USER = "test-minio-user"
config_mock.SECRET_MINIO_DATA_PIPELINE_PASSWORD = "test-minio-password"
config_mock.SIMPLIFIONS_MINIO_USER = "test-simplifions-minio-user"
config_mock.SIMPLIFIONS_MINIO_PASSWORD = "test-simplifions-minio-password"

_config_patcher = patch.dict(
    "sys.modules",
    {
        "datagouvfr_data_pipelines.config": config_mock,
    },
)
_config_patcher.start()
