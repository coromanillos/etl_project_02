# test_config.py

import pytest
import yaml
from src.utils.config import load_config

@pytest.fixture
def valid_config(tmp_path):
    config = {"db": {"host": "localhost", "port": 5432}}
    path = tmp_path / "config.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return str(path), config

def test_load_valid_config(valid_config):
    config_path, expected = valid_config
    result = load_config(config_path)
    assert result == expected

def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent.yaml")

def test_load_config_invalid_yaml(tmp_path):
    config_path = tmp_path / "bad.yaml"
    config_path.write_text("db: [unclosed list")
    with pytest.raises(yaml.YAMLError):
        load_config(str(config_path))

def test_load_config_from_env(monkeypatch, tmp_path):
    config = {"env": True}
    config_path = tmp_path / "env.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    result = load_config()
    assert result == config
