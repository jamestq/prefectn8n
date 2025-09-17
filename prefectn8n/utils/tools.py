from pathlib import Path
from rich import print
import yaml

def get_config(config_file: str, flow_name) -> dict:
    print(f"Using config file: {config_file}")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        print(f"Config loaded: {config}")
    if not check_valid_flow_name(config, "clean_data"):
        raise ValueError(f"[red]No config found for {flow_name}[/red]. Exiting...")    
    return config[flow_name]
    

def check_flow_config(config: dict, required_keys: list[str]) -> bool:
    missing_keys: list[str] = []
    for key in required_keys:
        if key not in config:
            missing_keys.append(key) if key not in config else None
    if len(missing_keys) > 0:
        raise ValueError(f"Missing required config keys: {', '.join(missing_keys)}")
    return True

def check_valid_flow_name(config: dict, flow_name: str) -> bool:
    return flow_name in config

def get_path(path_str: str) -> Path:
    return Path(path_str)

def get_save_path(path_str: str) -> Path:
    path = Path(path_str)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return path