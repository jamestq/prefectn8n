from prefect import task
import pandas as pd
from prefectn8n.tasks.mods import read_data
from sentence_transformers import SentenceTransformer 

@task
def compute_similarity(df: pd.DataFrame) -> pd.DataFrame:    
    model = SentenceTransformer('all-MiniLM-L6-v2')
    reference_sentences = df["Reference_Transcript"].tolist()
    scored_sentences = df["Scored_Transcript"].tolist()
    reference_embeddings = model.encode(reference_sentences)
    scored_embeddings = model.encode(scored_sentences)
    similarities = model.similarity(reference_embeddings, scored_embeddings)
    pair_wise = []
    for i in range(len(reference_sentences)):
        pair_wise.append(float(similarities[i][i]))
    df_sim = pd.DataFrame(pair_wise, columns=["Similarity"])
    new_df = pd.concat([df, df_sim], axis=1)
    return new_df

@task
def compute_gotoh_distance(df: pd.DataFrame) -> pd.DataFrame:
    import gotoh
    data = list()
    for _, row in df.iterrows():    
        counts = gotoh.counts(row["Reference_Transcript"], row["Scored_Transcript"])
        data.append((row["ID"], *counts))        
    df_match = pd.DataFrame(data, columns=["ID", "matches", "mismatches", "gaps", "spaces"]) 
    df_match["total_length"] = df_match[["matches", "mismatches", "gaps"]].sum(axis=1)
    df_gotoh = pd.merge(df, df_match, on="ID", how="left")
    print(df_gotoh.head())
    df_gotoh.sort_values(by="matches", inplace=True, ascending=False)    
    return df_gotoh

@task
def train_test_split(path: str):
    df = read_data(path)