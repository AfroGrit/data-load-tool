import requests
import json

def download_and_yield_rows(url):
    """
    Downloads and yields rows from a given URL.
    
    This function sends a GET request to the provided URL and yields each line in the response. 
    It raises an HTTPError for bad responses.
    
    Parameters:
    url (str): The URL to send the GET request to.

    Yields:
    dict: The next row from the response.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return

    for line in response.iter_lines():
        if line:
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Failed to parse line: {e}")
                continue

# Replace the URL with your actual URL
url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

# Use the generator to iterate over rows with minimal memory usage
try:
    for row in download_and_yield_rows(url):
        # Process each row as needed
        print(row)
except Exception as e:
    print(f"An error occurred: {e}")
