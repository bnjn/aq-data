import re

import requests
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import redis
import numpy as np


class PollutionData:

    def __init__(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("pollution-data")
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel('ERROR')

    def close(self):
        self.spark.stop()

    def transform_data(self, data_field):
        input_data = data_field.collect()

        output_data = []

        for station in input_data:
            output_data.append({
                "uid": station[4],
                "aqi": station[0],
                "station_name": station[3]['name'],
                "latitude": station[1],
                "longitude": station[2],
                "last_updated": station[3]['time']
            })

        return output_data

    def get_pollution_data(self, api_key, bbox):
        url = f'https://api.waqi.info/map/bounds/?token={api_key}&latlng={bbox}'
        response = requests.get(url)
        if response.status_code == 200:
            if response.json()['status'] == 'ok':
                return response.json()['data']
            else:
                raise ValueError(response.json()['data'])
        else:
            raise Exception('server error')

    def set_pollution_data(self, data, host, port, password):
        r = redis.StrictRedis(host=host, port=port, decode_responses=True, password=password)

        for station in data:
            if re.search("United Kingdom", station["station_name"]):
                r.hset(f'{station["station_name"]}:{station["uid"]}', mapping=station)
