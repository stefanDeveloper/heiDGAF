from abc import abstractmethod
import redis


class Analyzer():
    def __init__(self) -> None:
        self.redis_client = redis.Redis(connection_pool=pool)
