# src/shared/utils.py
import yaml
from pathlib import Path

def load_config(config_path: str = "config/settings.yaml") -> dict:
    """
    Loads the YAML config file and returns a dictionary.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found at {path.absolute()}")
    
    with open(path, "r") as f:
        return yaml.safe_load(f)

# Usage Example:
# config = load_config()
# headless_mode = config['acquisition']['twitter']['headless']