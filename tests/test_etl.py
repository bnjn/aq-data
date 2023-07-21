import unittest
from etl.etl import PollutionData
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession


class SparkETLTestCase(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Unit-tests")
                      .getOrCreate())
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed <socket.socket")

    def tearDown(self):
        self.spark.sparkContext.stop()
        self.spark.stop()

    def test_etl_transform_data(self):
        input_schema = StructType([
            StructField('lat', FloatType(), False),
            StructField('lon', FloatType(), False),
            StructField('uid', IntegerType(), False),
            StructField('aqi', StringType(), False),
            StructField('station', StructType([
                StructField('name', StringType(), False),
                StructField('time', StringType(), False)
            ]), False),
        ])

        input_data = [{
            "lat": 50.90814,
            "lon": -1.395778,
            "uid": 3211,
            "aqi": "25",
            "station": {
                "name": "Southampton Centre, United Kingdom",
                "time": "2023-07-20T23:00:00+09:00"
            }
        }]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = StructType([
            StructField('uid', IntegerType(), False),
            StructField('aqi', StringType(), False),
            StructField('station_name', StringType(), False),
            StructField('latitude', FloatType(), False),
            StructField('longitude', FloatType(), False),
            StructField('last_updated', StringType(), False)
        ])

        expected_data = [
            (3211, "25", "Southampton Centre, United Kingdom", 50.90814, -1.395778, "2023-07-20T23:00:00+09:00")]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        pollution_data = PollutionData()
        transformed_df = pollution_data.transform_data(input_df)

        self.assertTrue(transformed_df.schema.fields == expected_df.schema.fields)

        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))
