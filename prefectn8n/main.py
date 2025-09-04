from prefectn8n.pipelines.preprocess import (
    clean_data,
    combine_data,        
)
from dotenv import load_dotenv
from prefect import serve

load_dotenv()

def app():
    clean_data_deploy = clean_data.to_deployment(name="clean-data")
    combine_data_deploy = combine_data.to_deployment(name="combine-data")
    serve(clean_data_deploy, combine_data_deploy) #type: ignore

if __name__ == "__main__":
    app()