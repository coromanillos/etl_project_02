#############################################
# Name: config.py
# Author: Christopher O. Romanillos
# Description: Central point of config access
# Date: 08/02/25
#############################################

import os
from dotenv import load_dotenv
import yaml

# Optional .env loading – no crash if missing
dotenv_path = os.path.join(os.path.dirname(__file__), "../../.env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # Optionally override or supplement with env vars
    api_url = os.environ.get("API_URL", config.get("api", {}).get("url"))
    api_key = os.environ.get("API_KEY", config.get("api", {}).get("key"))

    config["api"]["url"] = api_url
    config["api"]["key"] = api_key
    return config
