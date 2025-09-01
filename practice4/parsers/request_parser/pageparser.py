import aiohttp
import datetime

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

from bs4 import BeautifulSoup, ResultSet


class ParserAbc(ABC):
    """Базовый класс для парсинга html страницы"""

    @abstractmethod
    def parse_page(self, html: str,
                   start_date: Optional[date] = None,
                   end_date: Optional[date] = None) -> list[tuple[str, date]]:
        pass


class BaseHtmlParserService(ABC):

    """Базовый класс для работы со страницой"""

    @abstractmethod
    def __init__(self, url: str, parser: ParserAbc):
        pass

    @abstractmethod
    def get_html(self) -> str:

        """Получаем html из request"""

        pass

    @abstractmethod
    def get_links(self) -> list:

        """Получаем ссылки из html"""

        pass


class ParserPageLinks(ParserAbc):
    """Конкретная реализация парсера"""

    def parse_page(self, html: str,
                   start_date: Optional[date] = None,
                   end_date: Optional[date] = None) -> list[tuple[str, date]]:
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all("a",
                              class_="accordeon-inner__item-title link xls")
        results = self.extract_href(links=links,
                                    start_date=start_date,
                                    end_date=end_date)

        return results

    def extract_href(self,
                     links: ResultSet,
                     start_date: date = None,
                     end_date: date = None) -> list[tuple[str, date]]:

        """Вытаскиваем ссылки из links"""

        results = []
        for link in links:
            href = link.get("href")
            if not href:
                continue
            href = href.split("?")[0]
            if "/upload/reports/oil_xls/oil_xls_" not in href or not href.endswith(".xls"):
                continue
            try:
                date = href.split("oil_xls_")[1][:8]
                file = datetime.datetime.strptime(date, "%Y%m%d").date()
                if not (start_date and end_date) or (start_date <= file <= end_date):
                    u = href if href.startswith("http") else f"https://spimex.com{href}"
                    results.append((u, file))
                else:
                    print(f"Ссылка {href} вне диапазона дат")
                    if results:
                        return results
                    return None
            except Exception as e:
                print(f"Не удалось извлечь дату из ссылки {href}: {e}")
        return results


class AsyncXlsParserService(BaseHtmlParserService):
    def __init__(self,
                 parser: ParserAbc,
                 session: aiohttp.ClientSession):
        self.parser = parser
        self.session = session

    async def get_html(self,
                       url: str,
                       next_page: str = None,
                       page_number: int = None,) -> str:
        absolute_url = f'{url}{next_page}{page_number}'
        async with self.session.get(absolute_url) as response:
            response.raise_for_status()
            html = await response.text()
            return html
        return None

    async def get_links(self,
                        url: str,
                        next_page: str | None = None,
                        page_number: int | None = None,
                        start_date: Optional[date] = None,
                        end_date: Optional[date] = None) -> list[tuple[str, date]]:
        try:
            html = await self.get_html(url,
                                       next_page,
                                       page_number)
            if html:
                result = self.parser.parse_page(html,
                                                start_date,
                                                end_date)
            else:
                return None
        except Exception as e:
            print(e)
        else:
            return result
