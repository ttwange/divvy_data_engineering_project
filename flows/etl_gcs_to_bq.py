import os
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(period: int) -> Path:
    """Extracts data from gcs and saves it to local storage."""
    gcs_path = f"{period}-divvy-tripdata.csv"
    gcs_bucket = GcsBucket.load("divvy-gcs")
    gcs_bucket.get_directory(from_path=gcs_path, local_path=f"./gcs_data/")
    return Path(f"./gcs_data/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Transforms the raw csv file into a dataframe for analysis."""
    df = pd.read_csv(path)
    return df 


@task()
def load_bq(df: pd.DataFrame) -> None:
    """Loads transformed dataset in BigQuery table."""
    
    gcp_block = GcpCredentials.load("divvy-gcs-creds")    
    df.to_gbq(
        destination_table="divvy.rid",
        project_id="divvyproject-397220",
        credentials=gcp_block.get_credentials_from_service_account(),
        chunksize=20_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    """ Main ETL flow to load data into Big Query"""
    period = 202301

    path = extract_from_gcs(period)
    df = transform(path)
    load_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()