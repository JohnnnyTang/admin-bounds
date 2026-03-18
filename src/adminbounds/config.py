from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ADMINBOUNDS_DB_",
        env_file=".env",
        extra="ignore",
        populate_by_name=True,
    )

    host: str = "localhost"
    port: int = 5432
    name: str = "geo_prism"
    user: str = "postgres"
    password: str = ""
    # 'schema' is reserved by Pydantic; use alias to read ADMINBOUNDS_DB_SCHEMA
    db_schema: str = Field(default="public", alias="schema", validation_alias="ADMINBOUNDS_DB_SCHEMA")


@lru_cache
def get_settings() -> Settings:
    return Settings()


def make_settings(**kwargs) -> Settings:
    """Build Settings from explicit kwargs, falling back to env vars."""
    return Settings(**{k: v for k, v in kwargs.items() if v is not None})
