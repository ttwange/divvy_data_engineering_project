import os
import requests
import zipfile

def download_zip_file(url, download_path):
    """
    Download a zip file from the given URL.

    Args:
        url (str): The URL of the zip file.
        download_path (str): The full path where the downloaded zip file will be saved.

    Returns:
        str: The path to the downloaded zip file.
    """
    # Create the download directory if it doesn't exist
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    response = requests.get(url)
    
    if response.status_code == 200:
        with open(download_path, 'wb') as file:
            file.write(response.content)
        return download_path
    else:
        raise Exception(f"Failed to download the zip file from {url}.")

# Example usage
zip_url = "https://divvy-tripdata.s3.amazonaws.com/202004-divvy-tripdata.zip"
zip_download_path = ".\divvy\data\202004-divvy-tripdata.zip"
downloaded_path = download_zip_file(zip_url, zip_download_path)
print(f"Zip file downloaded to: {downloaded_path}")

# Unzip the downloaded file and save the CSV
zip_extracted_path = ".\divvy\data"
with zipfile.ZipFile(downloaded_path, "r") as zip_ref:
    zip_ref.extractall(zip_extracted_path)
    print("Zip file extracted.")

# Assuming the CSV file name follows a specific pattern, find it
csv_file_name = [name for name in os.listdir(zip_extracted_path) if name.endswith(".csv")]
if csv_file_name:
    csv_file_path = os.path.join(zip_extracted_path, csv_file_name[0])
    print(f"CSV file found: {csv_file_path}")
else:
    print("No CSV file found in the extracted folder.")
