from typing import Annotated, Literal

from pydantic import BeforeValidator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

STDLIB_LEVEL_ALIASES = {"WARN": "WARNING", "FATAL": "CRITICAL"}


def _normalise_level(value: object) -> object:
    if not isinstance(value, str):
        return value
    name = value.strip().upper()
    return STDLIB_LEVEL_ALIASES.get(name, name)


LogLevel = Annotated[
    Literal["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"],
    BeforeValidator(_normalise_level),
]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://orders:orders@localhost:55432/orders"
    valkey_url: str = "redis://localhost:56379/0"
    db_connect_timeout_seconds: float = Field(default=5, gt=0, allow_inf_nan=False)
    db_command_timeout_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=0, ge=0)

    orders_stream: str = "orders.accepted"
    stock_group: str = "stock"
    relay_enabled: bool = True
    relay_batch_size: int = Field(default=100, ge=1, le=1000)
    poll_interval_seconds: float = Field(default=0.5, gt=0, allow_inf_nan=False)

    log_level: LogLevel = "INFO"
    log_json: bool = False


settings = Settings()
