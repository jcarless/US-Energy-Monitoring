import logging
import requests
import dateutil.parser

from airflow.hooks.S3_hook import S3Hook

EIA_API_KEY = '6674b1b8335bf22ca494e401e196db18'
URL = f"http://api.eia.gov/series/"


def process_generation(SERIES_ID):
    generation_data = get_generation(SERIES_ID)
    save_generation(generation_data)
    return


def process_demand(SERIES_ID):
    demand_data = get_demand(SERIES_ID)
    save_demand(demand_data)
    return


def save_generation(generation_data, bucket_name='us-energy'):
    s3 = S3Hook()

    key = f'generation/{generation_data.split(",")[0]}'

    return s3.load_string(generation_data,
                          key=key,
                          bucket_name=bucket_name,
                          encoding='utf-8')


def save_demand(demand_data, bucket_name='us-energy'):
    s3 = S3Hook()

    key = f'demand/{demand_data.split(",")[0]}'

    return s3.load_string(demand_data,
                          key=key,
                          bucket_name=bucket_name,
                          encoding='utf-8')


def get_generation(SERIES_ID):

    PARAMS = {
        'num': 1,
        'api_key': EIA_API_KEY,
        'series_id': SERIES_ID
    }

    r = requests.get(url=URL, params=PARAMS)
    data = r.json()

    ts = str(dateutil.parser.parse(data["series"][0]["data"][0][0]))
    net_generation = str(data["series"][0]["data"][0][1])

    generation_data = ts+","+net_generation

    return generation_data


def get_demand(SERIES_ID):

    PARAMS = {
        'num': 1,
        'api_key': EIA_API_KEY,
        'series_id': SERIES_ID
    }

    r = requests.get(url=URL, params=PARAMS)
    data = r.json()

    ts = str(dateutil.parser.parse(data["series"][0]["data"][0][0]))
    demand = str(data["series"][0]["data"][0][1])

    demand_data = ts+","+demand

    return demand_data


if __name__ == "__main__":
    GENERATION_SERIES_ID = 'EBA.FLA-ALL.NG.H'
    DEMAND_SERIES_ID = 'EBA.FLA-ALL.D.H'

    print(process_demand(DEMAND_SERIES_ID))
