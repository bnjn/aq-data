import requests
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import redis


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
        input_data = data_field.collect()[0]

        output_data = [{
            "uid": input_data['uid'],
            "aqi": input_data['aqi'],
            "station_name": input_data['station']['name'],
            "latitude": input_data['lat'],
            "longitude": input_data['lon'],
            "last_updated": input_data['station']['time']
        }]

        output_schema = StructType([
            StructField('uid', IntegerType(), False),
            StructField('aqi', StringType(), False),
            StructField('station_name', StringType(), False),
            StructField('latitude', FloatType(), False),
            StructField('longitude', FloatType(), False),
            StructField('last_updated', StringType(), False)
        ])

        return self.spark.createDataFrame(data=output_data, schema=output_schema)

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

    def set_pollution_data(self, data_frame, host, port, password):
        r = redis.StrictRedis(host=host, port=port, decode_responses=True, password=password)
        return r.hset('pollution-data:123', mapping=data_frame.collect())
