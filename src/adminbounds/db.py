import psycopg2
from sqlalchemy import create_engine, Engine

from .config import Settings


def get_engine(settings: Settings) -> Engine:
    s = settings
    url = f"postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.name}"
    return create_engine(url)


def get_raw_connection(settings: Settings) -> psycopg2.extensions.connection:
    s = settings
    return psycopg2.connect(
        host=s.host,
        port=s.port,
        dbname=s.name,
        user=s.user,
        password=s.password,
    )
