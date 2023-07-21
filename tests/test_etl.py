import unittest
from etl.etl import transform_data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

class SparkETLTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*}")
                     .appName("Unit-tests")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_etl(self):
        input_schema = StructType([
            StructField('lat', IntegerType(), False),
            StructField('lon', IntegerType(), False),
            StructField('uid', IntegerType(), False),
            StructField('aqi', StringType(), False),
            StructField('station', StructType([
                StructField('name', StringType(), False),
                StructField('time', StringType(), False)
            ]), False),
        ])

        input_data = {
            "lat": 50.90814,
            "lon": -1.395778,
            "uid": 3211,
            "aqi": "25",
            "station": {
                "name": "Southampton Centre, United Kingdom",
                "time": "2023-07-20T23:00:00+09:00"
            }
        }

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = StructType([
            StructField('uid', IntegerType(), False),
            StructField('aqi', StringType(), False),
            StructField('station_name', StringType(), False),
            StructField('latitude', IntegerType(), False),
            StructField('longitude', IntegerType(), False),
            StructField('last_updated', StringType(), False)
        ])

        expected_data = [(3211, "25", "Southampton Centre, United Kingdom", 50.90814, -1.395778, "2023-07-20T23:00:00+09:00")]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        transformed_df = transform_data(input_df)

        field_list = lambda fields: (fields.name, fields.type, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]

        res = set(fields1) == set(fields2)

        self.assertTrue(res)

        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))
