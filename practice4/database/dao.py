from sqlalchemy import insert

from .config import Base, async_session


class TradingDao():
    def __init__(self, model: Base):
        self.model = model

    async def insert_all_data(self, data: list[dict]):
        mappings = [item.model_dump() for item in data]
        async with async_session() as session:
            await session.execute(insert(self.model), mappings)
            await session.commit()