import yaml
from pathlib import Path

def load_config(config_file="config/settings.yaml"):
    """Loads a YAML configuration file."""
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    return config