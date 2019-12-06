import logging
import requests
from datetime import datetime
import io
import gzip
import json

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


URL = f"https://api.tomtom.com/traffic/services"


def load_traffic_incident_details(bounding_box=None, api_key=None, bucket_name='real-time-traffic', s3_connection='s3_connection', **kwargs):
    if api_key is None:
        print("Loading API Key...")
        api_key = BaseHook.get_connection("tomtom_api").password

    if bounding_box is None:
        print("Loading Bound Box...")
        bounding_box = Variable.get("usa_bounding_box")

    traffic_incident_details_data = get_traffic_details(bounding_box, api_key)
    save_traffic_incident_details(traffic_incident_details_data, bucket_name, s3_connection)


def save_traffic_incident_details(data, bucket_name, s3_connection):
    print("Uploading to s3...")

    try:
        s3 = S3Hook(aws_conn_id=s3_connection)
        date_time = datetime.utcnow().replace(microsecond=0).isoformat()

        key = f'traffic_details/raw/{date_time}.json.gz'

        s3.load_bytes(zip_data(data),
                      key=key,
                      bucket_name=bucket_name)

    except BaseException as e:
        print("Failed to upload to s3!")
        raise e


def zip_data(data):
    print("Zipping incedent details...")
    try:
        gz_body = io.BytesIO()
        gz = gzip.GzipFile(None, 'wb', 9, gz_body)
        gz.write(data.encode('utf-8'))
        gz.close()
        return gz_body.getvalue()

    except BaseException as e:
        print("Zip failed!")
        raise e


def get_traffic_details(bounding_box, api_key):

    print("Getting incedent details...")

    try:
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
        print("Failed to extract incedent details from API!")
        raise e


if __name__ == "__main__":
    api_key = BaseHook.get_connection("tomtom_api").password
    bounding_box = Variable.get("usa_bounding_box")
    load_traffic_incident_details(bounding_box, api_key)
