import asyncio

from aiohttp import ClientSession
from io import BytesIO

download_semaphore = asyncio.Semaphore(10)

async def load_xls_bytes(url: str, session: ClientSession) -> BytesIO:
    async with download_semaphore:
        async with session.get(url) as file_response:
            file_response.raise_for_status()
            xls_file = await file_response.read()
            return xls_file
