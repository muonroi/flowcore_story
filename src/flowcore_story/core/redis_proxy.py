import random

import redis.asyncio as redis


class RedisProxyPool:
    def __init__(self, redis_url="redis://localhost:6379/0", proxy_key="proxy_pool"):
        self.redis_url = redis_url
        self.proxy_key = proxy_key
        self.redis = None

    async def connect(self):
        self.redis = await redis.from_url(self.redis_url, decode_responses=True)

    async def add_proxy(self, proxy):
        await self.redis.sadd(self.proxy_key, proxy)  #type: ignore

    async def get_all(self):
        proxies = await self.redis.smembers(self.proxy_key) #type: ignore
        return list(proxies)

    async def get_random_proxy(self):
        proxies = await self.get_all()
        if not proxies:
            return None
        return random.choice(proxies)

    async def remove_proxy(self, proxy):
        await self.redis.srem(self.proxy_key, proxy) #type: ignore

    async def close(self):
        if self.redis:
            await self.redis.close()
