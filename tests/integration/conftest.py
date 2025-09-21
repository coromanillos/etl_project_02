# conftest for integration testing, loads the config-test.yaml

import pytest
import yaml
from pathlib import Path
from unittest.mock import MagicMock

@pytest.fixture(scope="session")
def test_config():
    """Load config-test.yaml for integration tests."""
    config_path = Path(__file__).parent.parent / "config-test.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

@pytest.fixture
def mock_logger():
    """Reusable mock logger."""
    return MagicMock()
