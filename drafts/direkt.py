import json
import re
from time import sleep

import requests
import numpy as np
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def import_direkt_data(token, dates: tuple, goals: list, field_names: list, client_login=None):
    """Получает данные из Директа. Возвращает массив данных отчета.
    Params -
    token: токен доступа;
    client_login: логин клиента;
    dates: кортеж из дат начала и конца диапазона;
    field_names: список необходимых полей для выгрузки."""

    reports_url = 'https://api.direct.yandex.com/json/v5/reports'

    # --- Подготовка запроса ---
    # Создание HTTP-заголовков запроса
    headers = {
        "Authorization": "Bearer " + token,
        "Accept-Language": "ru",
        "processingMode": "offline",
        "returnMoneyInMicros": "false",
        "skipReportHeader": "true",
        "skipReportSummary": "true"
    }
    if client_login is not None:                            # Добавление логина к заголовкам, если он передан
        headers['Client-Login'] = client_login

    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": dates[0],
                "DateTo": dates[1]
            },
            "Goals": goals,
            "FieldNames": field_names,
            "ReportName": f"ПО_КАМПАНИЯМ_{dates[0]}_{dates[1]}",
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    body = json.dumps(body, indent=4)

    # --- Запуск цикла для выполнения запросов ---
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросы
    while True:
        try:
            req = requests.post(reports_url, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break
            elif req.status_code == 200:
                print("Отчет создан успешно")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                return req.text
                break
            elif req.status_code == 201:
                print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print(
                    "Пожалуйста, попробуйте изменить параметры запроса:"
                    "уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            break


def parse_direkt_tsv(result):
    """Парсит TSV-ответ Директа в list для Google Таблиц. Возвращает полученные данные в формате датафрейма."""
    values = result.split('\n')
    values_new = []
    for i in range(len(values)):
        value = values[i].split('\t')
        values_new.append(value)

    values_new = pd.DataFrame(values_new,
                              columns=values_new[0],
                              )
    return values_new
