"""foo"""
import logging
import os
from http import HTTPStatus

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

api_url = "https://api.api-ninjas.com/v1/motorcycles"
api_token = os.environ["MOTORCYCLES_API_KEY"]
headers = {"X-Api-Key": api_token}

manufacturers = ["Motomel", "Zanella", "Honda", "Kawasaki", "Harley-Davidson"]
years = list(range(2022, 2023))

raw_motorcycles_data = []
for manufacturer in manufacturers:
    for year in years:
        params = {"make": manufacturer, "year": year}
        response = requests.get(api_url, params=params, headers=headers)
        if response.status_code != HTTPStatus.OK.value:
            logger.debug(
                f"Something when wrong when requesting data for {manufacturer} and year {year}"
            )
            pass

        raw_data = response.json()
        # There might not be any manufacturer information for the given year.
        # Do not append empty lists to the final result.
        if raw_data:
            raw_motorcycles_data.append(response.json())

# Flatten the extracted data because each one of the API responses is a list.
motorcycles_data = [
    motorcycle
    for raw_data_sublist in raw_motorcycles_data
    for motorcycle in raw_data_sublist
]
