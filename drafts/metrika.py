import logging

import pandas as pd

import json
import logging
import time
from abc import ABC, abstractmethod

import requests
from requests.exceptions import ReadTimeout


logger = logging.getLogger(__name__)


# Общие настройки для классов, которые забирают данные об индексации
class IndexSet:
    """Общие настройки для классов, которые забирают данные об индексации"""

    def __init__(self):
        self.token = os.getenv('YANDEX_TOKEN')
        self.start_date, self.end_date = self.get_period()

        self.MAX_RETRIES = 5
        self.TIMEOUT = 30

    @staticmethod
    def get_period():
        """Получает необходимый период для запроса данных из вебмастера"""

        last_year = date.today().year - 1
        start_date = str(date(last_year, 1, 1))
        end_date = str(date.today())

        return start_date, end_date


# Настройки подключения к Метрике
class MetrikaSet(IndexSet):
    """Настройки подключения к Метрике"""

    def __init__(self):
        """Инициализирует настройки Метрики"""
        IndexSet.__init__(self)

        self.counter_id = os.getenv('METRIKA_COUNTER')

        self.main_url = 'https://api-metrika.yandex.net/stat/'
        self.version = 'v1'


class IndexLoader:
    """Общие методы для получения данных об индексации страниц"""

    is_abstract = True

    def __init__(self, settings):
        self.settings = settings
        self.headers = {
            "Authorization": "OAuth " + self.settings.token,
        }

    def _get(self, endpoint, params):
        """Получает данные из Вебмастера по API. Кол-во попыток запроса данных ограничено константой MAX_RETRIES"""

        retries = 1

        while retries <= self.settings.MAX_RETRIES:
            try:
                result = requests.get(endpoint, headers=self.headers, params=params, timeout=self.settings.TIMEOUT)
                if result.status_code == 200:
                    return json.loads(result.text)
                else:
                    logger.warning(f'Can\'t get data from {endpoint}\n'
                                   f'Status code {result.status_code}\n'
                                   f'Response text {result.text}')
                    return
            except ReadTimeout:
                logger.warning(f'Attempt {retries} failed to get data from url {endpoint} in {self.settings.TIMEOUT} s. '
                               f'Retry in 1 min')
                retries += 1
                time.sleep(60)


class IndexReport(ABC):
    """Общий класс для формирования отчетов из Вебмастера и Метрики"""

    def __init__(self):
        self.data = None

    def add(self, data):
        """Добавляет данные для работы к объекту"""
        self.data = data

    def get(self):
        """Возвращает данные отчета"""
        return self.data

    @abstractmethod
    def clean(self):
        """Очищает и нормализует данные отчета"""
        pass


class MetrikaLoader(IndexLoader):
    """Класс для загрузки данных из Метрики"""

    is_abstract = False

    def get_unique_visitors(self):
        endpoint = f'/data'
        full_url = self.settings.main_url + self.settings.version + endpoint

        params = {
            'ids': self.settings.counter_id,
            'metrics': 'ym:s:users',
            'dimensions': 'ym:s:date',
            'date1': self.settings.start_date,
            'date2': self.settings.end_date,
            'accuracy': "full",
            'limit': 10000,
        }

        return self._get(full_url, params)


class MetrikaReport(IndexReport):
    """Класс для обработки данных из Метрики и подготовки их для загрузки в Google Таблицы"""

    def clean(self):
        """Очищает и нормализует данные отчета"""

        if not self.data:
            raise AttributeError('Please add data to the report')

        self.data = self.data.get('data')

        self.data = [{
            'dateTime': item.get('dimensions')[0].get('name'),
            'users': item.get('metrics')[0],
        } for item in self.data]

        self.data = pd.DataFrame(self.data).sort_values(by='dateTime')

