import sys
import logging
import datetime
import json
from collections import namedtuple
from pprint import pprint

import requests as r

import config
import config as c


def set_log_preferences(to_file=False, file_name='main_log.log'):
    """
    Функция задает настройки объекта логгирования.
    При вызове можно задать, будет ли производиться запись в файл или в консоль (по умолчанию в консоль).
    При записи в файл можно задать имя файла (по умолчанию main_log.log).
    :param to_file:bool - метка записи в файл. При False данные выводятся в консоль, иначе - пишутся в файл
    :param file_name:str - название файла, в который должны записываться логи
    :return: logger:logging.Logger - логгер
    """
    # TODO: разобраться с классом Formatter и переписать базовые настройки логгера без дублирования.

    logger = logging.Logger(__name__, level=config.level)

    if to_file:
        logging.basicConfig(
            filename=file_name,
            filemode='a',
            force=True,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        return logger

    logging.basicConfig(
        force=True,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    return logger


def get_test_dates():
    """
    Формирует кортеж тестовых дат начала и окончания периода типа datetime.datetime.
    :return: dates:Dates - именованный кортеж, содержащий два атрибута: first_date и second_date
    """
    first_date = datetime.datetime(2022, 10, 1)
    second_date = datetime.datetime(2022, 10, 7)

    Dates = namedtuple('Dates', ['first_date', 'second_date'])
    dates = Dates(first_date, second_date)

    return dates


class RoistatAPI:
    """
    Класс для получения данных из Ройстат по API.
    Адрес подключения к API является атрибутом класса, он задается в config.py.
    """
    rs_api_url = c.rs_api_url

    def __init__(self, project, token, logger=None):
        """
        Создает объект подключения к Ройстат API. При создании необходимо передать ID проекта и токен для подключения.
        Автоматически формирует headers для подключения и шаблон params.
        Если передается объект logger, то будут выводиться логи по ходу выполнения.
        :param project:str - ID проекта
        :param token:str - токен проекта
        :param logger:logging.Logger - логгер
        """
        # TODO: обработать случай, когда logger не передается.

        self.logger = logger

        self.headers = {
            'Api-key': token,
            'Content-type': 'application/json'
        }

        self.params = {
            'project': project
        }

    def _post_request(self, url, body, filters=None):
        """
        Метод отправляет post-запрос и возвращает текст ответа, если запрос выполнен успешно.

        :param url:str - url запроса
        :param body:dict - body запроса
        :param filters:dict - фильтры запроса
               Справка по фильтрам: https://help.roistat.com/API/methods/about/#api-_1
        :return: data:dict - текст ответа
        """
        # TODO: Обработать ситуацию, когда фильтры не переданы.

        self.logger.debug(f'API request for {url}')

        res = r.post(url,
                     headers=self.headers,
                     params=body,
                     json=filters,
                     timeout=30
                     )
        if res.status_code == 200:
            self.logger.debug(f'API Request successful')
        else:
            self.logger.error(f'Request failed with status code {res.status_code};\nresponse text: {res.text}')
            sys.exit(1)

        data = json.loads(res.text)
        return data

    def _get_request(self, url, body):
        """
        Метод отправляет get-запрос и возвращает текст ответа, если запрос выполнен успешно.

        :param url:str - url запроса
        :param body:dict - body запроса
        :return: data:dict - текст ответа
        """
        self.logger.debug(f'API request for {url}')

        res = r.get(url,
                    headers=self.headers,
                    params=body,
                    timeout=10
                    )
        if res.status_code == 200:
            self.logger.debug(f'API Request successful')
        else:
            self.logger.error(f'Request failed with status code {res.status_code};\nresponse text: {res.text}')
            sys.exit(1)

        data = json.loads(res.text)
        return data

    def get_visits(self, dates):
        """
        Получает по API данные о сделках за указанный период.
        Справка по методу: https://help.roistat.com/API/methods/visit/#list
        :param dates:Dates - именованный кортеж, содержащий два атрибута: first_date и second_date
        :return: visits - данные о визитах
        """
        method_url = '/project/site/visit/list'
        full_url = ''.join([self.__class__.rs_api_url, method_url])

        filters = {
            "filters": {
                "and": [
                    ["date", ">", dates.first_date.isoformat()],
                    ["date", "<", dates.second_date.isoformat()]
                ]
            }
        }

        res = self._post_request(full_url, self.params, filters=filters)

        # for item in res['data']:
        #     if item['source']['utm_source'] == 'yandex':
        #         pprint(item)
        #         break

        pprint(len(res['data']))

    def get_visit(self, visit_id):
        """
        Получает по API данные о конкретном визите по его id.
        Справка по методу: https://help.roistat.com/API/methods/visit/#list
        :param visit_id:str - id визита
        :return: data:dict - информация о визите
        """
        # TODO: Метод не работает, доразобраться с ним.

        method_url = '/project/site/visit/list'
        full_url = ''.join([self.__class__.rs_api_url, method_url])

        filters = {
            "filters": [
                'id', 'in', [visit_id]
            ]
        }

        res = self._post_request(full_url, self.params, filters=filters)
        pprint(res)

    def get_orders(self, dates):
        """
        Получает по API данные о сделках за указанный период.
        Справка по методу: https://help.roistat.com/API/methods/orders/#list
        :param dates:Dates - именованный кортеж, содержащий два атрибута: first_date и second_date
        :return: orders - данные о заказах
        """
        method_url = '/project/integration/order/list'
        full_url = ''.join([self.__class__.rs_api_url, method_url])

        filters = {
            "filters": {
                "and": [
                    ["creation_date", ">", dates.first_date.isoformat()],
                    ["creation_date", "<", dates.second_date.isoformat()]
                ]
            }
        }

        res = self._post_request(full_url, self.params, filters=filters)
        pprint(res['data'][0])

    def get_order(self, order_id):
        """
        Получает по API данные о конкретной сделке по ее id.
        Справка по методу: https://help.roistat.com/API/methods/orders/#info
        :param order_id:str - id сделки
        :return: data:dict - информация о сделке
        """
        method_url = f'/project/orders/{order_id}/info'
        full_url = ''.join([self.__class__.rs_api_url, method_url])

        res = self._get_request(full_url, self.params)
        pprint(res['order'])


if __name__ == '__main__':
    logger = set_log_preferences()
    dates = get_test_dates()

    rs = RoistatAPI(config.roistat_project, config.roistat_token, logger=logger)
    # rs.get_orders(dates)
    rs.get_visits(dates)

    # rs.get_order('42806932')
    # rs.get_visit('644839')
