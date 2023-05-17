import time
import logging
from pprint import pprint

import requests
import json
from datetime import date, datetime, timedelta
from typing import List

import pandas as pd

import config as c

logger = logging.getLogger(__name__)


def basic_config():
    # Задаем настройки вывода датафрейма в консоль
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    pd.options.mode.chained_assignment = None

    # Задаем настройки логгирования
    console_handler = logging.StreamHandler()

    logging.basicConfig(
        level=c.LOG_LEVEL,
        force=True,
        format=c.LOG_FORMAT,
        datefmt=c.DATE_FMT,
        handlers=[console_handler],
    )


class CallibriApi:
    """Класс для работы с API Callibri"""

    def __init__(self, start_date: str, finish_date: str, site_id: str = c.SITE_ID):
        """
        Создает объект для подключения к конкретному проекту.
        :param start_date: Дата начала периода выгрузки.
        :param finish_date: Дата окончания периода выгрузки.
        :param site_id: ID проекта в Callibri; по умолчанию подключается к проекту из конфига.
        """
        self.url = c.URL
        self.site_id = site_id
        self.params = None

        self.dates = []
        self._divide_period(start_date, finish_date)

    def _append_dates(self, start_date: date, finish_date: date):
        self.dates.append(
            tuple(
                map(
                    lambda x: x.strftime(c.DATE_FMT),
                    (start_date, finish_date)
                )
            )
        )

    def _divide_period(self, start_date: str, finish_date: str):
        """
        Делит весь период выгрузки данных на отрезки доступной длительности для Callibri
        (задаются в config.py переменной CALLIBRI_MAX_PERIOD).
        """

        start_date_before = datetime.strptime(start_date, c.DATE_FMT).date()
        finish_date_before = datetime.strptime(finish_date, c.DATE_FMT).date()

        delta = (finish_date_before - start_date_before).days

        if delta <= c.CALLIBRI_MAX_PERIOD:
            self.dates = [(start_date, finish_date)]
        else:
            temp_start_date = start_date_before

            while delta > c.CALLIBRI_MAX_PERIOD:
                temp_finish_date = temp_start_date + timedelta(days=(c.CALLIBRI_MAX_PERIOD-1))
                self._append_dates(temp_start_date, temp_finish_date)
                temp_start_date = temp_finish_date + timedelta(days=1)
                delta = (finish_date_before - temp_start_date).days

            temp_finish_date = temp_start_date + timedelta(days=(c.CALLIBRI_MAX_PERIOD - 1))
            self._append_dates(temp_start_date, temp_finish_date)

    def _stats_one_week(self, date1: str, date2: str):
        """
        Получает сырую статистику за неделю.
        :param date1: Дата начала периода выгрузки.
        :param date2: Дата окончания периода выгрузки.
        """

        self.params = {
            'user_email': c.USER_EMAIL,
            'user_token': c.TOKEN,
            'site_id': self.site_id,
            'date1': date1,
            'date2': date2,
        }

        result = requests.get(self.url, params=self.params)

        match result.status_code:
            case 200:
                res_json = json.loads(result.text)
                return res_json.get('channels_statistics')
            case _:
                print(result.status_code)
                print(result.text)

    def stats(self):
        """Получает сырую статистику за весь указанный период."""

        all_clients = []
        for date in self.dates:
            logger.info(f'Start portion from {date[0]} to {date[1]}')

            new_portion = self._stats_one_week(*date)
            all_clients.extend(new_portion)
            logger.info(f'End portion from {date[0]} to {date[1]}, sleep for {c.CALLIBRI_TIMEOUT} s')

            time.sleep(c.CALLIBRI_TIMEOUT)

        return all_clients

    def get_calls(self, stats):
        """Получает информацию по всем звонкам выбранного проекта"""

        calls = []
        for portion in stats:
            calls.extend(portion.get('calls'))

        return calls

        # calls_cleaned = []
        # for row in calls:
        #     temp_dict = {
        #         'date': row.get('date'),
        #         'phone': row.get('phone'),
        #         'duration': row.get('duration'),
        #     }
        #     calls_cleaned.append(temp_dict)
        #
        # return calls_cleaned

    @staticmethod
    def table_data(data: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame(data)
        # print(df)
        return df


def get_api_stats(date1: str, date2: str) -> list:

    url = 'https://api.callibri.ru/site_get_statistics'

    params = {
        'user_email': 'webcanape@yandex.ru',
        'user_token': c.TOKEN,
        'site_id': '12965',
        'date1': date1,
        'date2': date2,
    }

    result = requests.get(url, params=params)
    res_json = json.loads(result.text)

    channels_statistics = res_json.get('channels_statistics')

    clients = []
    for channel in channels_statistics:
        channel_data = []
        channel_data.extend(channel.get('calls'))
        channel_data.extend(channel.get('emails'))

        for row in channel_data:
            if row.get('utm_medium', None) == 'cpc' and row.get('conversations_number', None) == 1:
                temp_dict = {
                    'date': row.get('date', None),
                    'region': row.get('region', None),
                    'phone': row.get('phone', None),
                    'email': row.get('email', None),
                    'utm_source': row.get('utm_source', None),
                    'utm_medium': row.get('utm_medium', None),
                    'utm_campaign': row.get('utm_campaign', None),
                    'utm_content': row.get('utm_content', None),
                    'utm_term': row.get('utm_term', None),
                    'id': row.get('id', None),
                    'ym_uid': row.get('ym_uid', None),
                    'clbvid': row.get('clbvid', None),
                    'metrika_client_id': row.get('metrika_client_id', None),
                }
                clients.append(temp_dict)
    return clients


if __name__ == '__main__':

    basic_config()
    # ids = ['111']
    #
    # for id in ids:
    #     logger.info(f'Project {id}')
    #
    #     api = CallibriApi('01.04.2023', '23.04.2023', site_id=id)
    #     stats = api.stats()
    #
    #     with open('result.json', 'w') as f:
    #         json.dump(stats, f)

    with open('result.json') as f:
        data = json.load(f)

    result = []
    for item in data:
        result.extend(item.get('feedbacks'))

    result = pd.DataFrame(result,
                          columns=['id', 'date', 'source', 'email', 'traffic_type', 'utm_source', 'utm_medium',
                                   'utm_campaign', 'conversations_number', 'name', 'phone', 'ym_uid', 'clbvid',
                                   'ua_client_id', 'metrika_client_id']
                          )

    print(result)
    result.to_excel('result.xlsx')

