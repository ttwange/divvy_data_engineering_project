import os
import requests
import zipfile
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def download_zip_file(url, download_path):
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    response = requests.get(url)
    
    if response.status_code == 200:
        with open(download_path, 'wb') as file:
            file.write(response.content)
        return download_path
    else:
        raise Exception(f"Failed to download the zip file from {url}.")

@task(retries=3)
def extract_csv_from_zip(downloaded_path, zip_extracted_path):
    with zipfile.ZipFile(downloaded_path, "r") as zip_ref:
        csv_file_name = [name for name in zip_ref.namelist() if name.endswith(".csv")]
        if csv_file_name:
            zip_ref.extract(csv_file_name[0], path=zip_extracted_path)
            csv_file_path = os.path.join(zip_extracted_path, csv_file_name[0])
            print(f"CSV file extracted and saved: {csv_file_path}")
            return csv_file_path
        else:
            print("No CSV file found in the zip archive.")

@task()
def load_gcs(csv_path) -> None:
    """Upload data to Google cloud storage"""
    gcp_bucket = GcsBucket.load("divvy-gcs")
    gcp_bucket.upload_from_path(from_path=f"./data/{csv_path}", to_path=csv_path)
    return

@flow()
def elt_web_to_gcs(period: int) -> None:
    zip_url = f"https://divvy-tripdata.s3.amazonaws.com/{period}-divvy-tripdata.zip"
    zip_download_path = f"./data/{period}-divvy-tripdata.zip"
    zip_extracted_path = "./data"
    csv_path = f"{period}-divvy-tripdata.csv"
    downloaded_path = download_zip_file(zip_url, zip_download_path)
    csv_file_path = extract_csv_from_zip(downloaded_path, zip_extracted_path)
    print(f"Zip file downloaded to: {downloaded_path}")
    load_gcs(csv_path)

@flow()
def etl_parent_flow(
    periods: list[int] = [202005,202006]
):
    for period in periods:
        elt_web_to_gcs(period)

if __name__ == '__main__':
    periods = [202101,202102,202203]
    etl_parent_flow(periods)
