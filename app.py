import logging
import os

from airflow.api.client.local_client import Client
from airflow.exceptions import DagRunAlreadyExists, DagNotFound
from airflow.providers.postgres.hooks.postgres import PostgresHook
from flask import Flask

app = Flask(__name__)


def etl():
    client = Client(None, None)
    resp = client.trigger_dag(dag_id="etl_pipeline")
    logging.info(resp)


@app.route('/trigger-etl', methods=['GET'])
def trigger_etl():
    # Trigger your ETL process here
    try:
        etl()
    except DagRunAlreadyExists as e:
        logging.info("DagRunAlreadyExists: " + str(e))
        return {"message": "Trigger the API frequently, please try again later."}, 400
    except DagNotFound as e:
        logging.info("DagNotFound: " + str(e))
        return {"message": "Dag is loading, please try again later."}, 400
    return {"message": "ETL process started"}, 200


@app.route('/check/table', methods=['GET'])
def check_table():
    postgres_hook = PostgresHook(postgres_conn_id=os.environ.get("ETL_CONN_ID"))
    table_exists = postgres_hook.get_records("""
        SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'users_derivation');
    
    """)
    try:
        exist_bool = table_exists[0][0]
        assert exist_bool is True
    except (IndexError, AssertionError):
        return {"message": "Table creation failed"}, 500
    return {"message": "Table created"}, 200


@app.route('/check/feature_derivation', methods=['GET'])
def check_feature_derivation():
    postgres_hook = PostgresHook(postgres_conn_id=os.environ.get("ETL_CONN_ID"))
    result = postgres_hook.get_records("SELECT COUNT(*) FROM users_derivation;")
    try:
        record_num = result[0][0]
        assert record_num != 0
    except (IndexError, AssertionError):
        return {"message": "Feature Derivation insert failed"}, 500
    return {"message": "Feature Derivation inserted"}, 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
