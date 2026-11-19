from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://orders:orders@localhost:55432/orders"
    valkey_url: str = "redis://localhost:56379/0"


settings = Settings()
