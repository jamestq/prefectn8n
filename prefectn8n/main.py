from prefectn8n.pipelines.preprocess import prepare_data
from prefect import flow

@flow
def app():
    prepare_data()

if __name__ == "__main__":
    app.deploy(
        name="Prepare data flow",
    )