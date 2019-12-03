import logging
import requests
from datetime import datetime

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


URL = f"https://api.tomtom.com/traffic/services"


def load_traffic_incident_details(bounding_box=None, api_key=None, bucket_name='real-time-traffic', s3_connection='s3_connection', **kwargs):
    if api_key is None:
        print("Loading API Key")
        api_key = BaseHook.get_connection("tomtom_api").password

    if bounding_box is None:
        print("Loading Bound Box")
        bounding_box = Variable.get("usa_bounding_box")

    traffic_incident_details_data = get_traffic_details(bounding_box, api_key)
    save_traffic_incident_details(traffic_incident_details_data, bucket_name, s3_connection)
    return


def save_traffic_incident_details(traffic_incident_details_data, bucket_name, s3_connection):
    print("Saving to s3")
    s3 = S3Hook(aws_conn_id=s3_connection)
    date_time = datetime.utcnow().replace(microsecond=0).isoformat()

    key = f'traffic_details/raw/{date_time}'

    return s3.load_string(traffic_incident_details_data,
                          key=key,
                          bucket_name=bucket_name,
                          encoding='utf-8')


def get_traffic_details(bounding_box, api_key):

    try:
        print("Getting incedent details")
        PARAMS = {
            'projection': 'EPSG900913',
            'key': api_key,
            'expandCluster': 'false'
        }

        style = "s3"
        zoom = 7
        format = "json"
        versionNumber = 4
        url = f"{URL}/4/incidentDetails/{style}/{bounding_box}/{zoom}/-1/{format}"

        r = requests.get(url=url, params=PARAMS)
        r.raise_for_status()
        data = r.text

        return data
    except BaseException as e:
        raise e


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    TOMTOM_API_KEY = os.getenv('TOMTOM_API_KEY')
    usa_bounding_box = "2690124.773468,-15067267.015574,6300857.115604,-7015390.456013"

    print(load_traffic_incident_details(usa_bounding_box, TOMTOM_API_KEY))
