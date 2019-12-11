import logging
import requests
from datetime import datetime
import io
import gzip
import json
import unicodecsv as csv


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

    data_raw, date = get_incedent_details(bounding_box, api_key)
    data_transformed, date_transformed = transform_incedent_details(data_raw, date)
    load_to_s3(data_transformed, date_transformed, bucket_name, s3_connection)


def transform_incedent_details(data, date):
    traffic_model_id = data["tm"]["@id"]

    date_time = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z").replace(second=0, microsecond=0).isoformat()

    def category_switch(argument):
        switcher = {
            0: "Unknown",
            1: "Accident",
            2: "Fog",
            3: "Dangerous Conditions",
            4: "Rain",
            5: "Ice",
            6: "Jam",
            7: "Lane Closed",
            8: "Road Closed",
            9: "Road Works",
            10: "Wind",
            11: "Flooding",
            12: "Detour"
        }
        return switcher.get(argument, "Invalid Category")

    def magnitude_switch(argument):
        switcher = {
            0: "Unknown",
            1: "Minor",
            2: "Moderate",
            3: "Major",
            4: "Undefined"
        }
        return switcher.get(argument, "Invalid Magnitude")

    with io.BytesIO() as csvfile:
        writer = csv.writer(csvfile, encoding="utf-8")
        writer.writerow(["traffic_model_id",
                         "incedent_id",
                         "date",
                         "location"
                         "category",
                         "magnitude",
                         "description",
                         "estimated_end",
                         "cause",
                         "from_street",
                         "to_street",
                         "length",
                         "delay",
                         "road"])

        for cluster in data["tm"]["poi"]:
            if "cpoi" in cluster:
                for incedent in cluster["cpoi"]:

                    writer.writerow([
                        traffic_model_id,
                        incedent["id"],
                        date_time,
                        (incedent["p"]["x"], incedent["p"]["y"]),
                        category_switch(incedent["ic"]),
                        magnitude_switch(incedent["ty"]),
                        incedent["d"] if "d" in incedent else None,
                        incedent["ed"] if "ed" in incedent else None,
                        incedent["c"] if "c" in incedent else None,
                        incedent["f"] if "f" in incedent else None,
                        incedent["t"] if "t" in incedent else None,
                        incedent["l"] if "l" in incedent else None,
                        incedent["dl"] if "dl" in incedent else None,
                        incedent["r"] if "r" in incedent else None
                    ])

        csvfile.seek(0)
        zipped_file = gzip.compress(csvfile.getvalue(), compresslevel=9)

        return zipped_file, date_time


def load_to_s3(data, date, bucket_name, s3_connection):
    print("Uploading to s3...")

    try:
        s3 = S3Hook(aws_conn_id=s3_connection)

        key = f'traffic/incedent_details/{date}.csv.gz'

        s3.load_bytes(data,
                      key=key,
                      bucket_name=bucket_name)

    except BaseException as e:
        print("Failed to upload to s3!")
        raise e


def get_incedent_details(bounding_box, api_key):

    print("Getting incedent details...")

    try:
        PARAMS = {
            'projection': 'EPSG4326',
            'key': api_key,
            'expandCluster': 'true'
        }

        style = "s3"
        zoom = 7
        format = "json"
        versionNumber = 4
        url = f"{URL}/4/incidentDetails/{style}/{bounding_box}/{zoom}/-1/{format}"

        r = requests.get(url=url, params=PARAMS)
        r.raise_for_status()
        data = r.json()

        return data, r.headers["Date"]
    except BaseException as e:
        print("Failed to extract incedent details from API!")
        raise e


# DISTANCE BETWEEN TWO POINTS
# import geopy.distance


# coords_1 = (52.2296756, 21.0122287)
# coords_2 = (52.406374, 16.9251681)
# print geopy.distance.vincenty(coords_1, coords_2).miles
if __name__ == "__main__":
    api_key = BaseHook.get_connection("tomtom_api").password
    bounding_box = Variable.get("usa_bounding_box")
    load_traffic_incident_details(bounding_box, api_key)
