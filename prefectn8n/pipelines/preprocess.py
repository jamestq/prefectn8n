from fastapi import logger
from prefect import flow
from pathlib import Path
from prefectn8n.tasks.mods import (
    read_data,
    merge_dataframes,
    convert_to_str,
    lowercase_column,
    fill_na
)
import yaml

def get_config(config_file: str, flow_name) -> dict:
    print(f"Using config file: {config_file}")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        print(f"Config loaded: {config}")
    if check_valid_flow_name(config, "clean_data"):
        return config[flow_name]
    print("No 'clean_data' config found, exiting flow.")
    return {}

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

@flow
def clean_data(
        config_file: str = "config.yaml",
    ):
    config = get_config(config_file, "clean_data")  
    if not config:
        return      
    if "path" not in config or "save_path" not in config:
        raise ValueError("Please provide a valid config with 'path' and 'save_path' key.")
    dataset = get_path(config["path"])
    save_path = get_save_path(config["save_path"])
    df = read_data(dataset)
    if "id" in config:
        df = convert_to_str(df, config["id"])
    if "fillna" in config and type(config["fillna"]) is dict:
        for col, val in config["fillna"].items():
            df = fill_na(df, col, val)    
    if "lowercase" in config and type(config["lowercase"]) is list:
        for col in config["lowercase"]:
            df = lowercase_column(df, col)    
    df.to_csv(Path(save_path), index=False)

@flow
def combine_data(
    config_file: str = "config.yaml"
):
    config = get_config(config_file, "combine_data")  
    if not config:
        return      
    if "save_path" not in config:
        raise ValueError("Please provide a valid config with 'save_path' key.") 
    save_path = get_save_path(config["save_path"])
    if "datasets" not in config:
        raise ValueError("Please provide a valid config with 'datasets' key.")
    datasets: dict = config["datasets"]
    if "key" not in datasets or "how" not in datasets:
        raise ValueError("Please provide a valid config with 'key' and 'how' for merging datasets. Key can either be a string or a list of strings. How can be 'inner', 'outer', 'left', or 'right'.")
    files = [Path(p) for p in datasets["files"]]
    key_data = datasets["key"]
    final_df = read_data(files[0])
    for id in range(1, len(files)):
        next_df = read_data(files[id])
        key = key_data if type(key_data) is str else str(key_data[id - 1])
        final_df = merge_dataframes(final_df, next_df, on=key, how=datasets["how"])
    final_df.to_csv(save_path, index=False)

@flow
def save_data():
    utterances = read_data("data/scored_utterances.csv")
    ref_utterances = read_data("data/referenced_utterances.csv")
    df = merge_dataframes(utterances, ref_utterances, on="Line", how="inner")
    df = convert_to_str(df, "ID")
    df = fill_na(df, "Scored_Transcript", "")
    df = lowercase_column(df, "Scored_Transcript")
    df = lowercase_column(df, "Reference_Transcript")   
    df.to_csv("data/cleaned_utterances.csv", index=False)