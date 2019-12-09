import requests
import pytz
import datetime
import io
import csv
import json

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook

from helper_functions import zip_json


URL = "https://api.darksky.net/forecast"


def load_forecast(lon, lat, city, api_key=None, bucket_name='real-time-traffic', s3_connection="s3_connections"):
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
                   s3_connection=s3_connection)

    load_to_s3(type="current",
               city=city,
               data=forecast_transformed,
               bucket_name=bucket_name,
               s3_connection=s3_connection)


def load_to_s3(type, city, data, bucket_name, s3_connection):
    try:
        zipped_data = zip_json(data)
        s3 = S3Hook(aws_conn_id=s3_connection)
        date_time = datetime.datetime.fromtimestamp(data["time_utc"]).replace(second=0, microsecond=0).isoformat()
        key = f'traffic/weather/{city}/{type}/{date_time}.json.gz'

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


if __name__ == "__main__":
    load_forecast(40.7127837, -74.0059413, "New York")
