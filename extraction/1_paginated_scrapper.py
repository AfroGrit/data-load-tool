import requests
import time
import logging

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
SLEEP_DURATION = 1  # Pause duration between requests

logging.basicConfig(level=logging.INFO)


def paginated_scrapper():
    page_number = 1

    while True:
        params = {'page': page_number}

        try:
            response = requests.get(BASE_API_URL, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            break

        page_json = response.json()
        logging.info(
            f'Got page number {page_number} with {len(page_json)} records')

        if page_json:
            yield page_json
            page_number += 1
            time.sleep(SLEEP_DURATION)  # Pause between requests
        else:
            break


if __name__ == '__main__':
    for page_data in paginated_scrapper():
        print(page_data)
