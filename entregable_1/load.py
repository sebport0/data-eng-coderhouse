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


def build_sql_motorcycles_data_insert(motorcycles_data: list[dict]) -> str:
    """Since the COPY command can only be used with files stored in S3,
    we are going to perform a multi-row insert of the form:

        insert into <my_table> values
        (x, y, default, z),
        (a, b, c, d),
        ...,
        (foo, bar, woo, rar);

    Where default is just the default value for the column in case
    data is missing. The only required columns make, model, year
    and type.
    """
    sql = f"""insert into {my_schema}.{table_name} values """
    total_motorcycles = len(motorcycles_data)
    for i in range(total_motorcycles):
        separator = ";" if i == total_motorcycles - 1 else ","
        displacement = motorcycles_data[i].get("displacement")
        sql_displacement = f"'{displacement}'" if displacement else "default"

        engine = motorcycles_data[i].get("engine")
        sql_engine = f"'{engine}'" if engine else "default"

        top_speed = motorcycles_data[i].get("top_speed")
        sql_top_speed = f"'{top_speed}'" if top_speed else "default"

        power = motorcycles_data[i].get("power")
        sql_power = f"'{power}'" if power else "default"

        compression = motorcycles_data[i].get("compression")
        sql_compression = f"'{compression}'" if compression else "default"

        bore_stroke = motorcycles_data[i].get("bore_stroke")
        sql_bore_stroke = f"'{bore_stroke}'" if bore_stroke else "default"

        cooling = motorcycles_data[i].get("cooling")
        sql_cooling = f"'{cooling}'" if cooling else "default"

        fuel_consumption = motorcycles_data[i].get("fuel_consumption")
        sql_fuel_consumption = (
            f"'{fuel_consumption}'" if fuel_consumption else "default"
        )

        emission = motorcycles_data[i].get("emission")
        sql_emission = f"'{emission}'" if emission else "default"

        front_suspension = motorcycles_data[i].get("front_suspension")
        sql_front_suspension = (
            f"'{front_suspension}'" if front_suspension else "default"
        )

        rear_suspension = motorcycles_data[i].get("rear_suspension")
        sql_rear_suspension = f"'{rear_suspension}'" if rear_suspension else "default"

        front_tire = motorcycles_data[i].get("front_tire")
        sql_front_tire = f"'{front_tire}'" if front_tire else "default"

        rear_tire = motorcycles_data[i].get("rear_tire")
        sql_rear_tire = f"'{rear_tire}'" if rear_tire else "default"

        front_brakes = motorcycles_data[i].get("front_brakes")
        sql_front_brakes = f"'{front_brakes}'" if front_brakes else "default"

        rear_brakes = motorcycles_data[i].get("rear_brakes")
        sql_rear_brakes = f"'{rear_brakes}'" if rear_brakes else "default"

        dry_weight = motorcycles_data[i].get("dry_weight")
        sql_dry_weight = f"'{dry_weight}'" if dry_weight else "default"

        total_height = motorcycles_data[i].get("total_height")
        sql_total_height = f"'{total_height}'" if total_height else "default"

        total_length = motorcycles_data[i].get("total_length")
        sql_total_length = f"'{total_length}'" if total_length else "default"

        total_width = motorcycles_data[i].get("total_width")
        sql_total_width = f"'{total_width}'" if total_width else "default"

        starter = motorcycles_data[i].get("starter")
        sql_starter = f"'{starter}'" if starter else "default"

        sql += (
            f"('{motorcycles_data[i]['make']}',"
            f"'{motorcycles_data[i]['model']}',"
            f"{motorcycles_data[i]['year']},"
            f"'{motorcycles_data[i]['type']}',"
            f"{sql_displacement},"
            f"{sql_engine},"
            f"{sql_power},"
            f"{sql_top_speed},"
            f"{sql_compression},"
            f"{sql_bore_stroke},"
            f"{sql_cooling},"
            f"{sql_fuel_consumption},"
            f"{sql_emission},"
            f"{sql_front_suspension},"
            f"{sql_rear_suspension},"
            f"{sql_front_tire},"
            f"{sql_rear_tire},"
            f"{sql_front_brakes},"
            f"{sql_rear_brakes},"
            f"{sql_dry_weight},"
            f"{sql_total_height},"
            f"{sql_total_length},"
            f"{sql_total_width},"
            f"{sql_starter})"
        )
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
            motorcycles_data
        )
        cursor.execute(insert_motorcycles_data_sql)
    except Exception:
        logger.error("Something went wrong with the table creation or the data insert!")

    cursor.close()
    connection.close()
    logger.info("Done.")
