import os

import redshift_connector


def create_motorcycles_sql(schema: str, table_name: str = "motorcycles") -> str:
    return f"""
        create table if not exists {schema}.{table_name} (
            make varchar NOT NULL,
            model varchar NOT NULL,
            year date NOT NULL,
            type varchar NOT NULL,
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


if __name__ == "__main__":
    connection = redshift_connector.connect(
        host=os.environ["REDSHIFT_CODER_HOST"],
        database=os.environ["REDSHIFT_CODER_DB"],
        port=int(os.environ["REDSHIFT_CODER_PORT"]),
        user=os.environ["REDSHIFT_CODER_USER"],
        password=os.environ["REDSHIFT_CODER_PASSWORD"],
    )

    connection.rollback()
    connection.autocommit = True
    cursor = connection.cursor()

    my_schema = "sebassi_coderhouse"
    table_name = "motorcycles"

    cursor.execute(create_motorcycles_sql(my_schema, table_name))

# cursor.execute(f"INSERT INTO {my_schema}.test99 VALUES (6)")
# values = cursor.execute(f"SELECT * FROM {my_schema}.test99").fetchall()
# print(values)
