from datetime import datetime, date as dt

from pydantic import BaseModel


class TradingShema(BaseModel):
    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: float
    total: float
    count: int
    date: datetime
    created_on: datetime
    updated_on: datetime
