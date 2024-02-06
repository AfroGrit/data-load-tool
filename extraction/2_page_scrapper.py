import requests
import json
import logging

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

logging.basicConfig(level=logging.INFO)


def download_and_read_page(url):
    """
    This function downloads and reads a page from a given URL.
    It handles request exceptions and logs any errors.
    The function also parses each line of the response and yields the parsed line.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return

    for line in response.iter_lines():
        if line:
            parsed_line = json.loads(line.decode('utf-8'))
            yield parsed_line


if __name__ == '__main__':
    downloaded_data = list(download_and_read_page(url))
    if downloaded_data:
        # Log the first 5 entries as an example
        logging.info(downloaded_data[:5])
