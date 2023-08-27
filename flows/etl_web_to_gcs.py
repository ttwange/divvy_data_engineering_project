from pathlib import Path
import pandas as pd
from prefect import task,flow
from prefect_gcp.cloud_storage import GcsBucket


@task()
def fetch(dataset: str) -> pd.DataFrame:
    """Fetch a dataset from web to pandas dataframe"""
    df = pd.read_csv(dataset)
    return df

@flow()
def et_web_to_gcs() -> None:
    """The main ETL function"""
    period = 202004
    dataset = "https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv?accessType=DOWNLOAD"
    

    df = fetch(dataset)

if __name__ == "__main__":
    et_web_to_gcs()