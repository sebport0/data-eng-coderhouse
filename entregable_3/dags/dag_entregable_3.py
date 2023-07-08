import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

API_URL = "https://api.api-ninjas.com/v1/motorcycles"
MANUFACTURERS = ["Motomel", "Zanella", "Honda", "Kawasaki", "Harley-Davidson"]
YEARS = list(range(2015, 2023))


@dag(
    dag_id="entregable_3",
    schedule=None,
    start_date=datetime(2023, 5, 30),
    dagrun_timeout=timedelta(minutes=10),
    tags=["coder-entregables"],
)
def etl():
    @task
    def extract(manufacturers: list[str], years: list[int], api_url: str) -> str:
        import json
        from http import HTTPStatus

        import requests

        api_token = Variable.get("MOTORCYCLES_API_KEY")
        headers = {"X-Api-Key": api_token}

        logger.info("Requesting raw motorcycle data...")
        raw_motorcycles_data = []
        for manufacturer in manufacturers:
            for year in years:
                logger.info(f"Manufacturer {manufacturer}, year {year}.")
                params = {"make": manufacturer, "year": year}
                response = requests.get(api_url, params=params, headers=headers)

                if response.status_code != HTTPStatus.OK:
                    logger.error(
                        f"Something when wrong when requesting data from {manufacturer} in year {year}"
                    )
                    pass

                raw_data = response.json()
                # There might not be any manufacturer information for the given year.
                # Do not append empty lists to the final result.
                if raw_data:
                    raw_motorcycles_data.append(response.json())

        # Flatten the extracted data because each one of the API responses is a list[dict].
        motorcycles_data = [
            motorcycle
            for raw_data_sublist in raw_motorcycles_data
            for motorcycle in raw_data_sublist
        ]

        filename = "motorcycles.json"
        logger.info(f"Saving the data in a JSON file {filename}")
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(motorcycles_data, file)

        return filename

    @task
    def transform(filename: str):
        import json

        logger.info("Loading data from the extract step...")
        logger.info(f"Using file: {filename}")
        with open(filename, "r") as file:
            data: list[dict] = json.load(file)
        print("Transform")

    @task
    def load(x):
        print("Load")

    data_filename = extract(MANUFACTURERS, YEARS, API_URL)
    processed_motorcycles_data = transform(data_filename)
    load(processed_motorcycles_data)


etl()
