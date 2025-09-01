import aiohttp
from time import time
import asyncio 

from datetime import datetime, date
from database.shemas import TradingShema
from database.dao import TradingDao
from database.model import Trading
from parsers.request_parser.pageparser import AsyncXlsParserService, ParserPageLinks
from parsers.xls_parser import pandas_module
from parsers.async_load_xls import load_xls_bytes
from const import URL, NEXT_PAGE_URL, PAGE_COUNTS

def create_shemas(parsed_xls) -> list[TradingShema]:
    to_db = []
    try:
        for _, row in parsed_xls.iterrows():
            # Создаем схему для модели
            xls_model = TradingShema(
                exchange_product_id=str(row['Код Инструмента']),
                exchange_product_name=str(row['Наименование Инструмента']),
                delivery_basis_name=str(row['Базис поставки']),
                volume=float(row['Объем Договоров в единицах измерения']),
                total=int(row['Обьем Договоров, руб.']),
                count=int(row['Количество Договоров, шт.']),
                oil_id=str(row['Код Инструмента'][:4]),
                delivery_basis_id=str(row['Код Инструмента'][4:7]),
                delivery_type_id=str(row['Код Инструмента'][-1]),
                date=datetime.now(),
                created_on=datetime.now(),
                updated_on=datetime.now())
            to_db.append(xls_model)
    except Exception as e:
        print(e)
        if to_db:
            return to_db
        return None
    else:
        return to_db

async def gen_page_links(xls, page_number):
    for number in range(1, page_number+1):
        task = await xls.get_links(url=URL, next_page=NEXT_PAGE_URL, page_number=number, start_date=date(year=2023, month=1, day=1), end_date=date.today())
        yield task
    

async def main():
    db_tasks = []
    links_tasks = []

    db_unit = TradingDao(model=Trading)
    async with aiohttp.ClientSession() as session:
        xls = AsyncXlsParserService(ParserPageLinks(), session)
        for page_number in range(1, PAGE_COUNTS+1):
            links_tasks.append(asyncio.create_task(xls.get_links(url=URL, next_page=NEXT_PAGE_URL, page_number=page_number, start_date=date(year=2023, month=1, day=1), end_date=date.today())))

        async for page in asyncio.as_completed(links_tasks):
            finished_page = await page
            try:
                xls_bytes = [] 
                for link in finished_page:
                    xls_bytes.append(asyncio.create_task(load_xls_bytes(link[0], session)))
                for loaded_xls in asyncio.as_completed(xls_bytes):
                    xls = await loaded_xls
                    parsed_xls = pandas_module.parse_xls(xls)
                    res = create_shemas(parsed_xls)
                    db_tasks.append(asyncio.create_task(db_unit.insert_all_data(res)))
            except Exception as e:
                print(f'Exception. Page: {finished_page}, {e}')
        await asyncio.gather(*db_tasks)
    
if __name__ == '__main__':
    start = time()
    asyncio.run(main())
    print(f'Total time: {time()-start}s.')