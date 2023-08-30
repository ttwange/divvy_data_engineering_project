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
        else:
            print("No CSV file found in the zip archive.")

@task()
def load_gcs(extract_csv_path,csv_path) -> None:
    """Upload data to Google cloud storage"""
    gcp_bucket = GcsBucket.load("divvy-gcs")
    gcp_bucket.upload_from_path(from_path=f".\data\{csv_path}", to_path=extract_csv_path)
    return

@flow()
def elt_web_to_gcs() -> None:
    period = 202004
    zip_url = f"https://divvy-tripdata.s3.amazonaws.com/{period}-divvy-tripdata.zip"
    zip_download_path = f".\data\{period}-divvy-tripdata.zip"
    zip_extracted_path = ".\data"
    csv_path = ".\data\{period}-divvy-tripdata.csv"
    downloaded_path = download_zip_file(zip_url, zip_download_path)
    extract_csv_task = extract_csv_from_zip(downloaded_path, zip_extracted_path)
    print(f"Zip file downloaded to: {downloaded_path}")  # Move this line here
    load_gcs(extract_csv_task)

if __name__ == '__main__':
    elt_web_to_gcs()