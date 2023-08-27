import os
import requests
import zipfile
from prefect import flow, task
from datetime import timedelta

@task
def download_and_extract():
    zip_url = "https://divvy-tripdata.s3.amazonaws.com/202004-divvy-tripdata.zip"
    zip_download_path = ".\data\202004-divvy-tripdata.zip"
    zip_extracted_path = ".\data"

    # Download the ZIP file
    os.makedirs(os.path.dirname(zip_download_path), exist_ok=True)
    response = requests.get(zip_url)
    
    if response.status_code == 200:
        with open(zip_download_path, 'wb') as file:
            file.write(response.content)
    else:
        raise Exception(f"Failed to download the zip file from {zip_url}")

    # Unzip and save the CSV
    with zipfile.ZipFile(zip_download_path, "r") as zip_ref:
        csv_file_name = [name for name in zip_ref.namelist() if name.endswith(".csv")]
        if csv_file_name:
            zip_ref.extract(csv_file_name[0], path=zip_extracted_path)
            csv_file_path = os.path.join(zip_extracted_path, csv_file_name[0])
            return csv_file_path
        else:
            return None

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    download_and_extract_result = download_and_extract()
    
if __name__ == '__main__':
    etl_web_to_gcs()
