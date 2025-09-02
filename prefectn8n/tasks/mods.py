from prefect import task
import pandas as pd
from pathlib import Path

@task
def read_data(file: str) -> pd.DataFrame:
    file_path = Path(file)
    match file_path.suffix:
        case '.csv':
            df = pd.read_csv(file_path)
        case '.xlsx':
            df = pd.read_excel(file_path)
        case '.json':
            df = pd.read_json(file_path)
        case '.parquet':
            df = pd.read_parquet(file_path)
        case _:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")    
    return df

@task
def merge_dataframes(df_left: pd.DataFrame, df_right: pd.DataFrame, on: str, how: str) -> pd.DataFrame:
    merged_df = pd.merge(df_left, df_right, on=on, how=how)
    return merged_df

@task
def convert_to_str(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].astype(str).str.strip()
    return df   

@task
def fill_na(df: pd.DataFrame, column: str, value: str) -> pd.DataFrame:
    df[column] = df[column].fillna(value)
    return df

@task
def lowercase_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].str.lower()
    return df
