import os
import requests
from bs4 import BeautifulSoup


# seed files path
seed_path = "/Users/consultez/Desktop/swiss-transport-pipeline/swiss_transport/seeds"
# URL to get the data
page_url = "https://data.opentransportdata.swiss/fr/dataset/istdaten"


def compare_file_name(new_name: str, list_file_names: list):
    for f in list_file_names:
        if f == new_name:
            return True
        else:
            return False


def get_latest_file_name_on_opentransportdata(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Get the first resource item
    resource = soup.find("li", class_="resource-item")
    filename = resource.find("a", class_="heading")["title"]
    return filename


def download_data(url, filename, output_dir="swiss_transport/seeds"):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    # Get the first resource item
    resource = soup.find("li", class_="resource-item")
    filename = resource.find("a", class_="heading")["title"]

    # Get the download href
    download_url = resource.find("a", class_="resource-url-analytics")["href"]

    # create the output dir
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    # Download the file
    download_response = requests.get(download_url, stream=True)
    with open(filepath, "wb") as f:
        for chunk in download_response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Saved as: {filepath}")
    return filepath


# What need to run to get the latest part -- need to automate using Prefect

# get latest file name on opentranportdata
file_name_opendata = get_latest_file_name_on_opentransportdata(page_url)
# get the list of my file names in my seed dbt
list_file_name_local = os.listdir(seed_path)

# if the names is not in seed --> download otherwise do nothing.
if compare_file_name(file_name_opendata, list_file_name_local) == False:
    download_data(page_url, file_name_opendata)
else:
    print(f"{file_name_opendata} CSV file existed already")
