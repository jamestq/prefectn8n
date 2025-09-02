from prefectn8n.pipelines.preprocess import (
    prepare_data,
    save_data,
    yet_another_flow
)
from prefect import serve

def app():
    prepare_data_deploy = prepare_data.to_deployment(name="prepare-data")
    save_data_deploy = save_data.to_deployment(name="save-data")    
    yet_another_flow_deploy = yet_another_flow.to_deployment(name="yet-another-flow")
    serve(prepare_data_deploy, save_data_deploy, yet_another_flow_deploy) #type: ignore

if __name__ == "__main__":
    app()