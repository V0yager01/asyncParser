from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from loader import(DB_HOST, DB_PORT, DB_USER, DB_NAME, DB_PASSWORD)

POSTGRESS_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

async_engine = create_async_engine(url=POSTGRESS_URL)
async_session = async_sessionmaker(async_engine)


class Base(DeclarativeBase):
    pass