import requests
import pytz
import datetime

from airflow.hooks.base_hook import BaseHook


URL = "https://api.darksky.net/forecast"


def load_forecast(lon, lat, api_key=None):
    if api_key is None:
        print("Loading API Key...")
        api_key = BaseHook.get_connection("dark_sky").password

    forecast = get_forecast(lon, lat, api_key)
    forecast_transformed = transform_forecast(forecast)

    if hasattr(forecast, 'alerts'):
        alerts_transformed = transform_alerts(forecast)

    # upload_forecast(forecast_transformed)


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
        "time_utc": datetime.datetime.fromtimestamp(currently["time"]),
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

    print("CURRENT: ", current_weather)

    return current_weather


def transform_alerts(forecast):
    print("Transforming Alerts...")

    alerts = forecast["alerts"]

    alerts_transformed = {
        "lon": forecast["longitude"],
        "lat": forecast["latitude"],
        "time_utc": datetime.datetime.fromtimestamp(alerts["time"]),
        "expires": datetime.datetime.fromtimestamp(alerts["expires"]),
        "title": alerts["title"],
        "description": alerts["description"],
        "uri": alerts["uri"]
    }

    return alerts_transformed


if __name__ == "__main__":
    load_forecast(40.7127837, -74.0059413)
