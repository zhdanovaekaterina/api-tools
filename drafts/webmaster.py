import logging

import pandas as pd
import requests

from src.entities import indexing as indx

logger = logging.getLogger(__name__)


# Настройки подключения к Яндекс Вебмастеру
class WebmasterSet(IndexSet):

    def __init__(self):
        """Инициализирует настройки вебмастера"""
        IndexSet.__init__(self)

        self.client_id = os.getenv('WEBMASTER_CLIENT_ID')
        self.user_id = os.getenv('WEBMASTER_USER_ID')
        self.host_id = os.getenv('WEBMASTER_HOST_ID')

        self.main_url = 'https://api.webmaster.yandex.net/'
        self.version = 'v4'
        self.SITEMAP_URL = 'https://wakeup-s.com/sitemap.xml'
        self.ROBOTS_TXT = 'https://wakeup-s.com/robots.txt'


class WebmasterLoader(indx.IndexLoader):
    """Класс для загрузки данных из Вебмастера"""

    is_abstract = False

    def get_pages_history(self):
        """
        Получает историю индексирования страниц.
        Период запроса данных определяется переменными из объекта настроек.
        """

        endpoint = f'/user/{self.settings.user_id}/hosts/{self.settings.host_id}/search-urls/in-search/history'
        full_url = self.settings.main_url + self.settings.version + endpoint

        params = {
            'date_from': self.settings.start_date,
            'date_to': self.settings.end_date,
        }

        return self._get(full_url, params)

    def get_indexing_state(self):
        """Получает информацию о текущем количестве страниц в поиске и исключенных страниц."""

        endpoint = f'/user/{self.settings.user_id}/hosts/{self.settings.host_id}/summary'
        full_url = self.settings.main_url + self.settings.version + endpoint

        result = self._get(full_url, params=None)
        result = {
            'excluded_pages_count': [result.get('excluded_pages_count')],
            'searchable_pages_count': [result.get('searchable_pages_count')],
            'site_problems': [result.get('site_problems')]
        }

        result = pd.DataFrame(result)
        print(result)

    def get_sitemap_info(self):
        """Получает информацию о файле sitemap.xml"""

        endpoint = f'/user/{self.settings.user_id}/hosts/{self.settings.host_id}/sitemaps'
        full_url = self.settings.main_url + self.settings.version + endpoint

        all_sitemaps = self._get(full_url, params=None)
        all_sitemaps = all_sitemaps.get('sitemaps')

        if not all_sitemaps:
            raise AttributeError('There are no sitemaps in response')

        for item in all_sitemaps:
            if item.get('sitemap_url') == self.settings.SITEMAP_URL:
                return item

        return 'Sitemap not found'

    def get_robots_info(self):
        """Получает информацию о файле robots.txt"""

        endpoint = self.settings.ROBOTS_TXT

        headers = {'User-Agent': 'Mozilla'}

        result = requests.get(endpoint, headers=headers, timeout=self.settings.TIMEOUT)
        return result.text


class WebmasterIndexPagesReport(indx.IndexReport):
    """Обрабатывает данные об индексации страниц из Вебмастера"""

    def clean(self):
        """Очищает данные отчета по индексации страниц"""

        if not self.data:
            raise AttributeError('Please add data to the report')

        self.data = pd.DataFrame(self.data.get('history'))
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['date'] = self.data['date'].apply(lambda x: str(x)[:10])


class WebmasterSitemapAndRobotsReport(indx.IndexReport):
    """Обрабатывает данные о sitemap.xml из Вебмастера и robots.txt"""

    def __init__(self):
        indx.IndexReport.__init__(self)
        self.sitemap = None
        self.robots = None

    def add(self, data):
        self.sitemap = data[0]
        self.robots = data[1]

    def clean(self):
        """Очищает данные sitemap"""

        if not self.sitemap or not self.robots:
            raise AttributeError('Please add data to the report')

        robots_has_problems = self._robots_has_problems()

        self.data = [
            [
                'errors_count',
                'urls_count',
                'robots_has_problems',
            ],
            [
                self.sitemap.get('errors_count'),
                self.sitemap.get('urls_count'),
                robots_has_problems,
            ]
        ]

    def get(self):
        """Возвращает данные отчета"""
        return self.data

    def _robots_has_problems(self) -> int:
        """
        Проверяет файл robots.txt на наличие инструкции 'Disallow: /'
        Если ее не находит, возвращает 0, иначе 1.
        """

        rows = self.robots.split('\n')

        for row in rows:
            temp = row.replace(' ', '').lower()
            if temp == 'disallow:/':
                return 1

        return 0


if __name__ == '__main__':
    pass
