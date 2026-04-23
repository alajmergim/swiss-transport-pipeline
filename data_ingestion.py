import os
import requests
from bs4 import BeautifulSoup
import logging

# seed files path
seed_path = "/Users/consultez/Desktop/swiss-transport-pipeline/swiss_transport/raw_data"
# URL to get the data
page_url = "https://data.opentransportdata.swiss/fr/dataset/istdaten"


def compare_file_name(new_name: str, list_file_names: list):
    if new_name in list_file_names:
        logging.info(f"{new_name} exist already in the seed folder.")
        return True
    logging.info(f"{new_name} will be downloaded.")
    return False


def get_latest_file_name_on_opentransportdata(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Get the first resource item
    resource = soup.find("li", class_="resource-item")
    filename = resource.find("a", class_="heading")["title"]
    logging.info(f"{filename} is the last file uploaded by opentransportdata.")
    return filename


def download_data(url, filename, output_dir):
    logging.info("Start the downloading process...")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    # Get the first resource item
    resource = soup.find("li", class_="resource-item")
    filename = resource.find("a", class_="heading")["title"]

    # Get the download href
    download_url = resource.find("a", class_="resource-url-analytics")["href"]

    logging.info("URL to start downloading found...")

    # create the output dir
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    # Download the file
    download_response = requests.get(download_url, stream=True)
    with open(filepath, "wb") as f:
        for chunk in download_response.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info("Data are downloaded...")
    print(f"Saved as: {filepath}")
    return filepath


# What need to run to get the latest part -- need to automate using Prefect

if __name__ == "__main__":
    file_name_opendata = get_latest_file_name_on_opentransportdata(page_url)
    print(logging.info(f"the latest file : {file_name_opendata}"))
    list_file_name_local = os.listdir(seed_path)
    print(
        logging.info(f"if possible the file will be loaded here:{list_file_name_local}")
    )

    if not compare_file_name(file_name_opendata, list_file_name_local):
        download_data(page_url, file_name_opendata, seed_path)
    else:
        logging.info(f"{file_name_opendata} CSV file existed already")
