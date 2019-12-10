import requests
import pytz
import datetime
import io
import csv
import json
import gzip


from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

URL = "https://api.darksky.net/forecast"


def load_forecast_s3(lon, lat, city, api_key=None, bucket_name='real-time-traffic', s3_connection="s3_connection", **context):
    if api_key is None:
        print("Loading API Key...")
        api_key = BaseHook.get_connection("dark_sky").password

    forecast = get_forecast(lon, lat, api_key)
    forecast_transformed = transform_forecast(forecast)

    if hasattr(forecast, 'alerts'):
        alerts_transformed = transform_alerts(forecast)

        load_to_s3(type="alerts",
                   city=city,
                   data=alerts_transformed,
                   bucket_name=bucket_name,
                   s3_connection=s3_connection,
                   kwargs=context)

    print("KWARGS: ", context["ti"])

    load_to_s3(type="current",
               city=city,
               data=forecast_transformed,
               bucket_name=bucket_name,
               s3_connection=s3_connection,
               kwargs=context)


def load_to_s3(type, city, data, bucket_name, s3_connection, kwargs):
    try:
        print("Loading data to s3...")
        zipped_data = zip_json(data)
        s3 = S3Hook(aws_conn_id=s3_connection)
        date_time = datetime.datetime.fromtimestamp(data["time_utc"]).replace(second=0, microsecond=0).isoformat()
        key = f'traffic/weather/{city}/{type}/{date_time}.json.gz'

        kwargs["ti"].xcom_push(key=type, value=date_time)

        s3.load_bytes(zipped_data,
                      key=key,
                      bucket_name=bucket_name)

    except BaseException as e:
        print("Failed to load data to s3!")
        raise e


def get_forecast(lon, lat, api_key):
    print("Getting weather forecast...")

    try:
        url = f"{URL}/{api_key}/{lon},{lat}"
        r = requests.get(url=url)
        r.raise_for_status()
        data = r.json()

        return data
    except BaseException as e:
        print("Failed to extract forecast from API!")
        raise e


def transform_forecast(forecast):
    print("Transforming Current Weather...")

    currently = forecast["currently"]

    current_weather = {
        "time_utc": currently["time"],
        "timezone": forecast["timezone"],
        "lon": forecast["longitude"],
        "lat": forecast["latitude"],
        "summary": currently["summary"],
        "nearest_storm_distance": currently["nearestStormDistance"],
        "visibility": currently["visibility"],
        "temp": currently["temperature"],
        "apparent_temp": currently["apparentTemperature"],
        "wind_speed": currently["windSpeed"],
        "wind_gust": currently["windGust"],
        "uv_index": currently["uvIndex"],
        "cloud_cover": currently["cloudCover"],
        "precip_type": currently["precipType"] if hasattr(currently, 'precipType') else None,
        "precip_probability": currently["precipProbability"],
        "precip_intensity": currently["precipIntensity"],
        "precip_intensity_error": currently["precipIntensityError"] if hasattr(currently, 'precipIntensityError') else None
    }

    return current_weather


def transform_alerts(forecast):
    print("Transforming Alerts...")

    alerts = forecast["alerts"]

    alerts_transformed = {
        "lon": forecast["longitude"],
        "lat": forecast["latitude"],
        "time_utc": alerts["time"],
        "expires": datetime.datetime.fromtimestamp(alerts["expires"]),
        "title": alerts["title"],
        "description": alerts["description"],
        "uri": alerts["uri"]
    }

    return alerts_transformed


# def load_forecast_rds(s3_connection="s3_connections", **kwargs):
#     s3 = S3Hook(aws_conn_id=s3_connection)
#     postgres = PostgresHook(postgres_conn_id=postgres_conn)
#     s3_data = Variable.get("s3_data", deserialize_json=True)
#     export_date = kwargs["ti"].xcom_pull(key=table_name, task_ids="write_to_s3")

#     key = f"{s3_data['raw']}/gsc/{export_date}/{table_name}.tsv"

#     s3_response = s3.get_object(Bucket=s3_data['bucket'], Key=key)

#     streaming_body = s3_response["Body"].read()

#     file = io.StringIO(streaming_body.decode("utf-8"))


def zip_json(data):
    print("Zipping data...")
    try:
        gz_body = io.BytesIO()
        gz = gzip.GzipFile(None, 'wb', 9, gz_body)
        gz.write(json.dumps(data).encode('utf-8'))
        gz.close()
        return gz_body.getvalue()

    except BaseException as e:
        print("Zip failed!")
        raise e


if __name__ == "__main__":
    load_forecast_s3(40.7127837, -74.0059413, "New York")
