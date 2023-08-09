class MockRedis:
    def __init__(self, cache=None):
        if cache is None:
            self.cache = dict()

        self.cache = cache

    def mock_get(self, key):
        if key in self.cache:
            return self.cache[key]
        return None  # return nil

    def mock_set(self, key, value, *args, **kwargs):
        if self.cache:
            self.cache[key] = value
            return "OK"
        return None  # return nil in case of some issue

    def mock_hget(self, mock_hash, key):
        if mock_hash in self.cache:
            if key in self.cache[mock_hash]:
                return self.cache[mock_hash][key]
        return None  # return nil

    def mock_hset(self, mock_hash, key, value, *args, **kwargs):
        if self.cache:
            self.cache[mock_hash][key] = value
            return 1
        return None  # return nil in case of some issue

    def redis_exists(self, key):
        if key in self.cache:
            return 1
        return 0

    def redis_cache_overwrite(self, cache=None):
        if cache is None:
            self.cache = dict()
        self.cache = cache
