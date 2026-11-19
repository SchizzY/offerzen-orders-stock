from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://orders:orders@localhost:55432/orders"
    valkey_url: str = "redis://localhost:56379/0"
    db_connect_timeout_seconds: float = 5
    db_command_timeout_seconds: float = 10
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=0, ge=0)


settings = Settings()
