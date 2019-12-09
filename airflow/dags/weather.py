"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils import dates
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException


from operators.weather import load_forecast

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow() - timedelta(minutes=3),
    "email": "carless.jerome@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("load_weather_forecast", default_args=default_args, catchup=False, schedule_interval='@hourly')

postgres = PostgresHook(postgres_conn_id="rtf_postgres")
connection = postgres.get_conn()

with connection.cursor() as curs:
    try:
        query = "SELECT city_name, lon, lat FROM cities"
        curs.execute(query)
    except BaseException as e:
        connection.rollback()
        raise AirflowException(f"""Query {query} failed""")
    else:
        cities = curs.fetchall()

        for city, lon, lat in cities:
            city = city.replace(" ", "_")
            print("CITY: ", city)
            print("LON: ", lon)
            print("LAT: ", lat)

            load_traffic_incident_details = PythonOperator(
                task_id=f"load_{city}_forecast",
                python_callable=load_forecast,
                provide_context=True,
                op_kwargs={"lon": lon, "lat": lat},
                dag=dag,
            )

            load_traffic_incident_details
