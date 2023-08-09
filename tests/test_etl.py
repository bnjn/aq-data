import unittest
from unittest.mock import MagicMock, patch, Mock
from mocks.MockRedis import MockRedis
from etl.etl import PollutionData
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession


class SparkETLTestCase(unittest.TestCase):

    def test_etl_transform_data(self):
        spark = (SparkSession
                 .builder
                 .master("local[*]")
                 .appName("Unit-tests")
                 .getOrCreate())
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed <socket.socket")
        spark.sparkContext.setLogLevel('ERROR')

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

        input_df = spark.createDataFrame(data=input_data, schema=input_schema)

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
        expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

        pollution_data = PollutionData()
        transformed_df = pollution_data.transform_data(input_df)

        self.assertTrue(transformed_df.schema.fields == expected_df.schema.fields)

        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

        spark.sparkContext.stop()
        spark.stop()

    @patch('etl.etl.requests')
    def test_etl_get_pollution_data(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "ok",
            "data": [
                {
                    "lat": 53.732182,
                    "lon": -6.3200354,
                    "uid": 14661,
                    "aqi": "16",
                    "station": {
                        "name": "Drogheda, Co. Louth, Ireland",
                        "time": "2023-07-24T19:00:00+09:00"
                    }
                }
            ]
        }

        mock_requests.get.return_value = mock_response

        pollution_data = PollutionData()

        expected_data = pollution_data.get_pollution_data('123', '49.920172,-10.729866,59.476389,1.904411')
        self.assertIn('lat', expected_data[0])
        self.assertIn('lon', expected_data[0])
        self.assertIn('uid', expected_data[0])
        self.assertIn('aqi', expected_data[0])
        self.assertIn('station', expected_data[0])
        self.assertIn('name', expected_data[0]['station'])
        self.assertIn('time', expected_data[0]['station'])

    @patch('etl.etl.requests')
    def test_etl_get_pollution_data_invalid_bounds(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "error",
            "data": "invalid bounds"
        }

        mock_requests.get.return_value = mock_response

        pollution_data = PollutionData()

        with self.assertRaises(ValueError, msg='invalid bounds'):
            pollution_data.get_pollution_data('123', 'beef')

    @patch('etl.etl.requests')
    def test_etl_get_pollution_data_invalid_key(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "error",
            "data": "Invalid key"
        }

        mock_requests.get.return_value = mock_response

        pollution_data = PollutionData()

        with self.assertRaises(ValueError, msg='Invalid key'):
            pollution_data.get_pollution_data('123', 'beef')

    @patch('etl.etl.requests')
    def test_etl_get_pollution_data_error_code(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {}

        mock_requests.get.return_value = mock_response

        pollution_data = PollutionData()

        with self.assertRaises(Exception, msg='server error'):
            pollution_data.get_pollution_data('123', 'beef')

    @patch('redis.StrictRedis')
    def test_set_pollution_data(self, mock_redis):
        mock_set = MagicMock()
        mock_set = MockRedis({
            "foo": "bar"
        })

        mock_set.mock_hset = Mock(side_effect=mock_redis.mock_hset)

        pollution_data = PollutionData()

        spark = (SparkSession
                 .builder
                 .master("local[*]")
                 .appName("Unit-tests")
                 .getOrCreate())
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed <socket.socket")
        spark.sparkContext.setLogLevel('ERROR')

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

        input_df = spark.createDataFrame(data=input_data, schema=input_schema)

        self.assertTrue(pollution_data.set_pollution_data(input_df, port=1234, host='localhost', password='password'))

        spark.sparkContext.stop()
        spark.stop()