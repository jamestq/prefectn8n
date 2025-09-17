from prefectn8n.pipelines.preprocess import (
    clean_data,
    combine_data,            
    display_distribution
)
from dotenv import load_dotenv
from prefect import serve

load_dotenv()

def app():
    clean_data_deploy = clean_data.to_deployment(name="clean-data")
    combine_data_deploy = combine_data.to_deployment(name="combine-data")
    display_distribution_deploy = display_distribution.to_deployment(name="display-distribution")
    serve(combine_data_deploy, clean_data_deploy, display_distribution_deploy) #type: ignore

if __name__ == "__main__":
    app()