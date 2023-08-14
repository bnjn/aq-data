- Install `openjdk`
- Install `apache-spark`
- Create a venv and activate
- Install deps
- `python -m unittest discover tests` to test (not working right now)
- Create `.env` file in project root with the keys `POLLUTIONAPIKEY`, `REDISPASS`, `REDISHOST` and `REDISPORT`. `POLLUTIONAPIKEY` can be obtained from `https://aqicn.org/json-api`

### TODO
- clean up code and tests
- add requirements.txt for deps (requests, redis, pyspark, dotenv)
- figure out how to add spark back into data workflow
