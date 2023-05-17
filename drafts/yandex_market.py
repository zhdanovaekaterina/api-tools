import os

from dotenv import load_dotenv
load_dotenv()

import json
import logging
import time
from pprint import pprint

import requests
from requests.exceptions import ReadTimeout

logger = logging.getLogger(__name__)


class YandexMarketConfig:

    token = os.getenv('YANDEX_MARKET_TOKEN')
    campaign_id = os.getenv('YANDEX_MARKET_CAMPAIGN_ID')
    client_id = os.getenv('YANDEX_MARKET_CLIENT_ID')

    HOST = 'https://api.partner.market.yandex.ru'
    MAX_RETRIES = 5
    TIMEOUT = 30


class IndexLoader:
    """Общие методы для получения данных об индексации страниц"""

    is_abstract = True

    def __init__(self, settings):
        self.settings = settings
        self.headers = {
            "Authorization": "Bearer " + self.settings.token,
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

    def _post(self, endpoint, params):
        """Получает данные из Вебмастера по API. Кол-во попыток запроса данных ограничено константой MAX_RETRIES"""

        retries = 1

        while retries <= self.settings.MAX_RETRIES:
            try:
                data = json.dumps(params)
                result = requests.post(endpoint, headers=self.headers, data=data, timeout=self.settings.TIMEOUT)
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


class MetrikaLoader(IndexLoader):
    """Класс для загрузки данных из Метрики"""

    is_abstract = False

    def get_market_data(self):
        endpoint = f'/campaigns/{self.settings.campaign_id}/stats/orders'
        full_url = self.settings.HOST + endpoint

        params = {
            'dateFrom': '2023-04-01',
            'dateTo': '2023-04-15'
        }

        return self._post(full_url, params)


class MetrikaNewLoader(IndexLoader):

    def __init__(self, settings):
        IndexLoader.__init__(self, settings)

        self.headers = {
            "Authorization": f"OAuth oauth_token={self.settings.token}, oauth_client_id={self.settings.client_id}"
        }

    def get_market_data(self):
        endpoint = f'/campaigns/{self.settings.campaign_id}/stats/main'
        full_url = self.settings.HOST + '/v2' + endpoint

        params = {
            'fromDate': '01-04-2023',
            'toDate': '15-04-2023'
        }

        return self._get(full_url, params)


if __name__ == '__main__':
    loader = MetrikaLoader(YandexMarketConfig)

    data = loader.get_market_data()
    pprint(data)
