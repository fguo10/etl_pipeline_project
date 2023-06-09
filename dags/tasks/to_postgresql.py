import logging

import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.connection import Connection
from airflow import settings
from sqlalchemy.exc import IntegrityError
import os


# Create a Connection object
# postgresql+psycopg2://etl_user:etl_password@etl-pipeline-db/etl-pipeline
def create_connection():
    connection = Connection(
        conn_id=os.environ.get("ETL_CONN_ID"),
        conn_type=os.environ.get("ETL_CONN_TYPE"),
        description=None,
        login=os.environ.get("ETL_LOGIN"),
        password=os.environ.get("ETL_PASSWORD"),
        host=os.environ.get("ETL_HOST"),
        schema=os.environ.get("ETL_SCHEMA"),
    )

    # Add the connection to the Airflow metadata database
    session = settings.Session()
    try:
        session.add(connection)
        session.commit()
    except IntegrityError as e:
        logging.info(str(e))
        logging.info("Connection already exists.")
    else:
        logging.info("Connection created successfully.")
    finally:
        session.close()


def create_table():
    # Create a PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id=os.environ.get("ETL_CONN_ID"))
    # Create the users_derivation table if it doesn't exist
    create_sql = """
    CREATE TABLE IF NOT EXISTS users_derivation
    (
        user_id             NUMERIC PRIMARY KEY,
        name                VARCHAR,
        email               VARCHAR,
        signup_date         DATE,
        days_since_signup   INTEGER,
        experiments_count   INTEGER,
        experiment_run_time INTEGER
    );
    """
    postgres_hook.run(create_sql)


def insert_record(df):
    # Create a PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id=os.environ.get("ETL_CONN_ID"))

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Extract the values from the row
        user_id = row['user_id']
        name = row['name']
        email = row['email']
        signup_date = row['signup_date'].date()
        days_since_signup = row['days_since_signup']
        experiments_count = row['experiments_count']
        experiment_run_time = row['experiment_run_time']

        try:
            # Construct the INSERT statement
            insert_sql = """
            INSERT INTO users_derivation (user_id, name, email, signup_date, days_since_signup, experiments_count, experiment_run_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            values = (user_id, name, email, signup_date, days_since_signup, experiments_count, experiment_run_time)

            # Execute the INSERT statement
            postgres_hook.run(insert_sql, parameters=values)
        except psycopg2.errors.UniqueViolation as e:
            logging.info("record already exists")
