from typing import Annotated

from fastapi import Depends
from redis.asyncio import Redis

from app.config import settings

valkey = Redis.from_url(settings.valkey_url)


def get_valkey() -> Redis:
    return valkey


ValkeyDep = Annotated[Redis, Depends(get_valkey)]
