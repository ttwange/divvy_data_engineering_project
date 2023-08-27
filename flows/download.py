import os
import requests
import zipfile

def download_zip_file(url, download_path):
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    response = requests.get(url)
    
    if response.status_code == 200:
        with open(download_path, 'wb') as file:
            file.write(response.content)
        return download_path
    else:
        raise Exception(f"Failed to download the zip file from {url}.")

zip_url = "https://divvy-tripdata.s3.amazonaws.com/202004-divvy-tripdata.zip"
zip_download_path = ".\data\202004-divvy-tripdata.zip"
downloaded_path = download_zip_file(zip_url, zip_download_path)
print(f"Zip file downloaded to: {downloaded_path}")

# Unzip the downloaded file and save only the CSV
zip_extracted_path = ".\data"
with zipfile.ZipFile(downloaded_path, "r") as zip_ref:
    csv_file_name = [name for name in zip_ref.namelist() if name.endswith(".csv")]
    if csv_file_name:
        zip_ref.extract(csv_file_name[0], path=zip_extracted_path)
        csv_file_path = os.path.join(zip_extracted_path, csv_file_name[0])
        print(f"CSV file extracted and saved: {csv_file_path}")
    else:
        print("No CSV file found in the zip archive.")