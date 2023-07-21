from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


class PollutionData:

    def __init__(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("pollution-data")
                      .getOrCreate())

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
