from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags import *

"""
set AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

[core]
dags_are_paused_at_creation = False
"""

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

etl_dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
)

read_users_task = PythonOperator(
    task_id='read_users_task',
    python_callable=read_users_file,
    dag=etl_dag,
)

read_experiments_task = PythonOperator(
    task_id='read_experiments_task',
    python_callable=read_experiments_file,
    dag=etl_dag
)

user_feature_derivation_task = PythonOperator(
    task_id='user_feature_derivation_task',
    python_callable=user_feature_derivation,
    op_kwargs={'users_df': read_users_task.output, 'experiments_df': read_experiments_task.output},
    dag=etl_dag
)

create_connection_task = PythonOperator(
    task_id='create_connection',
    python_callable=create_connection,
    dag=etl_dag
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=etl_dag
)

insert_record_task = PythonOperator(
    task_id='insert_record',
    python_callable=insert_record,
    op_kwargs={'df': user_feature_derivation_task.output},
    dag=etl_dag
)

[read_users_task, read_experiments_task] >> user_feature_derivation_task >> insert_record_task
create_connection_task >> create_table_task >> insert_record_task
