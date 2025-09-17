from prefect import flow
from pathlib import Path
from prefectn8n.tasks.common import (
    read_data,
    merge_dataframes,
    convert_to_str,
    lowercase_column,
    fill_na
)
from prefectn8n.utils.tools import (
    get_config,
    get_path,
    get_save_path,
)

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
    if "fillna" in config and type(config["fillna"]) is dict:
        for col, method in config["fillna"].items():
            df = fill_na(df, col, method)    
    if "lowercase" in config and type(config["lowercase"]) is list:
        for col in config["lowercase"]:
            df = lowercase_column(df, col)    
    df.to_csv(Path(save_path), index=False)

@flow
def display_distribution(
    config_file: str = "config.yaml"
):
    """Display the distribution of values in a specified column of a dataset.
    
    Args:
        config_file (str, optional): Path to the configuration file. Defaults to "config.yaml".
    
    """
    config = get_config(config_file, "display_distribution")      
    if not config:
        return      
    if "path" not in config or "column" not in config:
        raise ValueError("Please provide a valid config with 'path' and 'column' key.")
    dataset = get_path(config["path"])
    column = config["column"]
    df = read_data(dataset)            
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in the dataset.")    
    # Display distribution as a histogram and save to file
    import seaborn as sns
    import matplotlib.pyplot as plt
    sns.histplot(data=df, x=column)
    plt.title(f'Distribution of {column}')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    if "save_path" in config:
        save_path = get_save_path(config["save_path"])
        plt.savefig(save_path)

@flow
def combine_data(
    config_file: str = "config.yaml"
):
    """Combine multiple datasets based on a common key.
    
    Args:
        config_file (str, optional): Path to the configuration file. Defaults to "config.yaml".
    
    """
    config = get_config(config_file, "combine_data")      
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


