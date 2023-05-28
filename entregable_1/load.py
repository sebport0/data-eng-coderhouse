import json
import logging
import os

import redshift_connector
from redshift_connector import Connection

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def connect_to_redshift() -> Connection:
    connection = redshift_connector.connect(
        host=os.environ["REDSHIFT_CODER_HOST"],
        database=os.environ["REDSHIFT_CODER_DB"],
        port=int(os.environ["REDSHIFT_CODER_PORT"]),
        user=os.environ["REDSHIFT_CODER_USER"],
        password=os.environ["REDSHIFT_CODER_PASSWORD"],
    )

    return connection


def create_motorcycles_table_sql(schema: str, table_name: str = "motorcycles") -> str:
    return f"""
        create table if not exists {schema}.{table_name} (
            make varchar not null,
            model varchar not null,
            year integer not null,
            type varchar not null,
            displacement varchar,
            engine varchar,
            power varchar,
            top_speed varchar,
            compression varchar,
            bore_stroke varchar,
            cooling varchar,
            fuel_consumption varchar,
            emission varchar,
            front_suspension varchar,
            rear_suspension varchar,
            front_tire varchar,
            rear_tire varchar,
            front_brakes varchar,
            rear_brakes varchar,
            dry_weight varchar,
            total_height varchar,
            total_length varchar,
            total_width varchar,
            starter varchar
        )
    """


def get_extracted_data_from_file(filename: str = "motorcycles.json") -> list[dict]:
    with open(filename, "r") as file:
        data: list[dict] = json.load(file)

    return data


def get_sql_statement(motorcycle_variable: str | int | None) -> str:
    return f"'{motorcycle_variable}'" if motorcycle_variable else "default"


def build_sql_motorcycles_data_insert(
    motorcycles_data: list[dict], schema: str, table_name: str
) -> str:
    """Since the COPY command can only be used with files stored in S3,
    we are going to perform a multi-row insert of the form:

        insert into <my_table> values
        (x, y, default, z),
        (a, b, c, d),
        ...,
        (foo, bar, woo, rar);

    Where default is just the default value for the column in case
    data is missing. The only required columns are 'make', 'model, 'year'
    and 'type'.
    """

    # Column order is important!
    columns = [
        "displacement",
        "engine",
        "power",
        "top_speed",
        "compression",
        "bore_stroke",
        "cooling",
        "fuel_consumption",
        "emission",
        "front_suspension",
        "rear_suspension",
        "front_tire",
        "rear_tire",
        "front_brakes",
        "rear_brakes",
        "dry_weight",
        "total_height",
        "total_length",
        "total_width",
        "starter",
    ]
    sql = f"""insert into {schema}.{table_name} values """
    total_motorcycles = len(motorcycles_data)
    for i in range(total_motorcycles):
        sql += (
            f"('{motorcycles_data[i]['make']}',"
            f"'{motorcycles_data[i]['model']}',"
            f"{motorcycles_data[i]['year']},"
            f"'{motorcycles_data[i]['type']}',"
        )
        for column in columns:
            column_sql = get_sql_statement(motorcycles_data[i].get(column))
            sql += column_sql
            # If in the last column, close the tuple.
            sql += "," if column != "starter" else ")"

        separator = ";" if i == total_motorcycles - 1 else ","
        sql += separator

    return sql


if __name__ == "__main__":
    logger.info("Connecting to Redshift...")
    connection = connect_to_redshift()
    connection.rollback()
    connection.autocommit = True
    cursor = connection.cursor()

    my_schema = "sebassi_coderhouse"
    table_name = "motorcycles"

    logger.info("Executing operations...")
    try:
        logger.info(f"Creating(if neccesary) the {table_name} table...")
        cursor.execute(create_motorcycles_table_sql(my_schema, table_name))

        logger.info(f"Retrieving data from file...")
        motorcycles_data = get_extracted_data_from_file()

        logger.info(f"Inserting data into Redshift...")
        insert_motorcycles_data_sql = build_sql_motorcycles_data_insert(
            motorcycles_data, my_schema, table_name
        )
        cursor.execute(insert_motorcycles_data_sql)
    except Exception as e:
        logger.error(
            f"Something went wrong with the table creation or the data insert! {e}"
        )

    logger.info("Closing connections...")
    cursor.close()
    connection.close()
    logger.info("Done.")
