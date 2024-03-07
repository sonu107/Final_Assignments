#Import required libraries.
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import pandas as pd
from pyspar_operations import pyspark_code

# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}



# Instantiate the DAG with the default arguments
with DAG(
    "Dagname",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    run_pyspark_job = PythonOperator(task_id="task2",
                                     provide_context=True,
                                     python_callable=pyspark_code,
                                     op_kwargs={
                                         "file_name":"csv_file.csv"
                                     }
                                     )

