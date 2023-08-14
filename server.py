from pyspark.sql.functions import *
from pyspark.sql.types import *

from etl.etl import PollutionData
from dotenv import dotenv_values

config = dotenv_values('.env')

server = PollutionData()

data = server.get_pollution_data(api_key=config['POLLUTIONAPIKEY'], bbox="49.920172,-10.729866,59.476389,1.904411")

df = server.spark.createDataFrame(data=data)

server.set_pollution_data(server.transform_data(df), config['REDISHOST'], config['REDISPORT'], config['REDISPASS'])

print('DONE :)')
