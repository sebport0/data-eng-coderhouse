import json
import logging
import os
from http import HTTPStatus

import requests

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_motorcycles_data_from_api(
    manufacturers: list[str], years: list[int]
) -> list[list[dict]]:
    api_url = "https://api.api-ninjas.com/v1/motorcycles"
    api_token = os.environ["MOTORCYCLES_API_KEY"]
    headers = {"X-Api-Key": api_token}

    raw_motorcycles_data = []
    for manufacturer in manufacturers:
        for year in years:
            logger.info(f"Manufacturer {manufacturer}, year {year}.")
            params = {"make": manufacturer, "year": year}
            response = requests.get(api_url, params=params, headers=headers)

            if response.status_code != HTTPStatus.OK.value:
                logger.error(
                    f"Something when wrong when requesting data from {manufacturer} in year {year}"
                )
                pass

            raw_data = response.json()
            # There might not be any manufacturer information for the given year.
            # Do not append empty lists to the final result.
            if raw_data:
                raw_motorcycles_data.append(response.json())

    return raw_motorcycles_data


def save_motorcycles_data(filename: str = "motorcycles.json"):
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(motorcycles_data, file, indent=4)


if __name__ == "__main__":
    manufacturers = ["Motomel", "Zanella", "Honda", "Kawasaki", "Harley-Davidson"]
    years = list(range(2015, 2023))

    logger.info("Requesting raw motorcycle data...")
    raw_motorcycles_data = get_motorcycles_data_from_api(manufacturers, years)

    # Flatten the extracted data because each one of the API responses is a list[dict].
    motorcycles_data = [
        motorcycle
        for raw_data_sublist in raw_motorcycles_data
        for motorcycle in raw_data_sublist
    ]

    logger.info(f"Saving the data in a JSON file.")
    save_motorcycles_data()

    logger.info("Done.")
