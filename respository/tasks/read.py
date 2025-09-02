from prefect import task
import pandas as pd

@task
def read_data(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    return df
