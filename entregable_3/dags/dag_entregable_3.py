import logging
from datetime import datetime, timedelta
from http import HTTPStatus

import boto3
import pyspark.sql.functions as F
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

API_URL = "https://api.api-ninjas.com/v1/motorcycles"
# MANUFACTURERS = ["Motomel", "Zanella", "Honda", "Kawasaki", "Harley-Davidson"]
MANUFACTURERS = ["Honda"]
YEARS = list(range(2015, 2023))


def save_in_s3(bucket: str, key: str, data: str):
    client = boto3.client(
        "s3",
        endpoint_url=Variable.get("S3_ENDPOINT_URL"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    client.put_object(Bucket=bucket, Key=key, Body=data)


def read_from_s3(bucket: str, key: str) -> str:
    client = boto3.client(
        "s3",
        endpoint_url=Variable.get("S3_ENDPOINT_URL"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


class ETL:
    @staticmethod
    def extract(manufacturers: list[str], years: list[int], api_url: str) -> list[dict]:
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

        return motorcycles_data

    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
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

        return transformed_df


def get_spark_session():
    spark = (
        SparkSession.builder.master("spark://spark:7077")
        # .config(
        #     "spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.12:3.2.0"
        # )
        # .config("spark.hadoop.fs.s3a.access.key", "test")
        # .config("spark.hadoop.fs.s3a.secret.key", "test")
        # .config("spark.hadoop.fs.s3a.endpoint", Variable.get(S3_ENDPOINT_URL))
        # .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    return spark


# TODO: documentation
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
            endpoint_url=Variable.get("S3_ENDPOINT_URL"),
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3_bucket = "entregable-3"
        logger.info(f"Attempting to create bucket {s3_bucket}...")

        try:
            _ = client.head_bucket(Bucket=s3_bucket)
            logger.info(f"Bucket {s3_bucket} already exists! Leaving it untouched.")
        except Exception:
            client.create_bucket(Bucket=s3_bucket)

        logger.info("Done.")
        return s3_bucket

    @task
    def get_motorcycles_data(
        s3_bucket: str, manufacturers: list[str], years: list[int], api_url: str
    ) -> dict[str, str]:
        import json

        motorcycles_data = ETL.extract(manufacturers, years, api_url)

        motorcycles_data_string = json.dumps(motorcycles_data)
        key = "data.json"
        save_in_s3(s3_bucket, key, motorcycles_data_string)

        return {"s3_bucket": s3_bucket, "key": key}

    @task
    def transform_motorcycles_data_with_spark(extract_response: dict) -> str:
        import json

        logger.info("Loading data from the extract step...")
        s3_bucket = extract_response["s3_bucket"]
        s3_key = extract_response["key"]
        raw_motorcycles_data = json.loads(read_from_s3(s3_bucket, s3_key))

        # Create Spark DataFrame.
        logger.info("Creating spark session...")
        spark = get_spark_session()

        logger.info("Building DataFrame from data...")
        df = spark.createDataFrame(raw_motorcycles_data)

        logger.info("Applying transformations...")
        ETL.transform(df)

        # Save transformed data as parquet in S3.
        # new_key = "transformed-data/data.json"
        # logger.info(
        #     f"Saving transformed data in S3 bucket {s3_bucket} with key {new_key}..."
        # )
        # transformed_df.write.json(f"s3a://{s3_bucket}/{new_key}", mode="overwrite")
        logger.info("Saving in JSON data.json...")
        filepath = "transformed_data.json"
        transformed_df.write.json(filepath, mode="overwrite")

        # TODO: save to S3 as transformed_df.json
        # motorcycles_data_string = json.dumps(motorcycles_data)
        # key = "motorcycles_data.json"
        # client.put_object(Bucket=s3_bucket, Key=key, Body=motorcycles_data_string)
        logger.info("Done.")

        return filepath

    @task
    def load(transform_response: str):
        print(f"Load {transform_response}")

    s3_bucket = create_s3_bucket()
    get_motorcycles_data_response = get_motorcycles_data(
        s3_bucket, MANUFACTURERS, YEARS, API_URL
    )
    transform_motorcycles_data_response = transform_motorcycles_data_with_spark(
        get_motorcycles_data_response
    )
    load(transform_motorcycles_data_response)


entregable_3()
