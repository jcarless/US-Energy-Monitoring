"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from operators.generation_consumption import process_generation, process_demand

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email": "carless.jerome@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("generation_demand_load", default_args=default_args, schedule_interval='@hourly')

process_generation = PythonOperator(
    task_id="process_generation",
    python_callable=process_generation,
    provide_context=True,
    dag=dag,
)

process_demand = PythonOperator(
    task_id="process_demand",
    python_callable=process_demand,
    provide_context=True,
    dag=dag,
)

[process_generation, process_demand]
