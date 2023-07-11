import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

API_URL = "https://api.api-ninjas.com/v1/motorcycles"
# MANUFACTURERS = ["Motomel", "Zanella", "Honda", "Kawasaki", "Harley-Davidson"]
MANUFACTURERS = ["Motomel"]
YEARS = list(range(2015, 2023))


# TODO: documentation
# TODO: import functions from another dir instead of writing them here.
# TODO: more tasks separation.
@dag(
    dag_id="entregable_3",
    schedule=None,
    start_date=datetime(2023, 5, 30),
    dagrun_timeout=timedelta(minutes=10),
    tags=["coder-entregables"],
)
def entregable_3():
    @task
    def create_s3_bucket() -> str:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url="http://localstack:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3_bucket = "entregable-3"

        try:
            response = client.head_bucket(Bucket=s3_bucket)
        except Exception:
            client.create_bucket(Bucket=s3_bucket)

        return s3_bucket

    @task
    def extract(
        s3_bucket: str, manufacturers: list[str], years: list[int], api_url: str
    ) -> dict[str, str]:
        import json
        from http import HTTPStatus

        import boto3
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

        # Save data in S3 bucket.
        client = boto3.client(
            "s3",
            endpoint_url="http://localstack:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        motorcycles_data_string = json.dumps(motorcycles_data)
        key = "motorcycles_data.json"
        client.put_object(Bucket=s3_bucket, Key=key, Body=motorcycles_data_string)

        return {"s3_bucket": s3_bucket, "key": key}

    @task
    def transform(extract_response: dict):
        import json

        import boto3
        import pyspark.sql.functions as F
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col
        from pyspark.sql.types import FloatType

        logger.info("Loading data from the extract step...")
        client = boto3.client(
            "s3",
            endpoint_url="http://localstack:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3_bucket = extract_response["s3_bucket"]
        s3_key = extract_response["key"]
        response = client.get_object(Bucket=s3_bucket, Key=s3_key)
        raw_motorcycles_data = json.loads(response["Body"].read())
        logger.info(f"{raw_motorcycles_data[0]}")

        spark = SparkSession.builder.master("spark://spark:7077").getOrCreate()
        df = spark.createDataFrame(raw_motorcycles_data)
        logger.info(df.columns)

        transformed_df = df.withColumn("year", col("year").cast("Integer"))

        def weight_in_kg(value):
            if value:
                return float(value.split(" kg")[0])
            return None

        udf_weight_in_kg = F.udf(weight_in_kg, FloatType())
        for column in ["dry_weight", "total_weight"]:
            transformed_df = transformed_df.withColumn(
                f"{column}_kg", udf_weight_in_kg(column)
            )

        # TODO: save to S3 as transformed_df.json
        # motorcycles_data_string = json.dumps(motorcycles_data)
        # key = "motorcycles_data.json"
        # client.put_object(Bucket=s3_bucket, Key=key, Body=motorcycles_data_string)

        # return s3_bucket, s3_data_key

    @task
    def load():
        print("Load")

    s3_bucket = create_s3_bucket()
    extract_response = extract(s3_bucket, MANUFACTURERS, YEARS, API_URL)
    transform(extract_response)
    load()


entregable_3()
