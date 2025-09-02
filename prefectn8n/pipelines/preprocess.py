from prefect import flow
from prefectn8n.tasks.mods import (
    read_data,
    merge_dataframes,
    convert_to_str,
    lowercase_column,
    fill_na
)

@flow
def prepare_data():
    utterances = read_data("data/scored_utterances.csv")
    ref_utterances = read_data("data/referenced_utterances.csv")
    df = merge_dataframes(utterances, ref_utterances, on="Line", how="inner")
    df = convert_to_str(df, "ID")
    df = fill_na(df, "Scored_Transcript", "")
    df.head()

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

@flow
def yet_another_flow():
    print("This is yet another flow.")