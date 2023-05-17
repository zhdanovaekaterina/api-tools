import json
import calendar
import logging
from datetime import datetime, timedelta, date
from time import sleep
from collections import namedtuple

import os
import json
import logging
from datetime import date, datetime, timedelta

import requests
import pandas as pd
import gspread as gs

from src.decorators import flood_error_proceed
from src.errors import FloodError

logger = logging.getLogger(__name__)


# Время, в которое необходимо запускать выполнение (строка в формате 24 ч)
do_at_str = os.getenv('DO_AT_HOUR').zfill(2) + ':' + os.getenv('DO_AT_MIN').zfill(2)

# Константы
LOG_FILENAME = './src/logs/logfile.txt'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging_level = logging.INFO

# Форматы дат
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
STRING_DATE_FORMAT = '%Y-%m-%d'
INPUT_DATE_FORMAT = 'YYYY-MM-DD'  # Отображение для ввода даты с клавиатуры
BX_DATE_FORMAT = '%d.%m.%Y'


# Настройки подключения к Google таблицам
class DatabaseSet:
    def __init__(self):

        key = os.getenv('GS_KEY_JSON')
        self.key = json.loads(key, strict=False)

        self.sheet_name = os.getenv('GS_BASE_SHEET_NAME')
        self.table_clients = os.getenv('BASE_TABLE_CLIENTS')
        self.table_balances = os.getenv('BASE_TABLE_BALANCES')
        self.table_spent = os.getenv('BASE_TABLE_SPENT')
        self.table_crm = os.getenv('BASE_TABLE_CRM')
        self.table_plan = os.getenv('BASE_TABLE_PLAN')
        self.table_deals_crm = os.getenv('BASE_TABLE_DEALS_CRM')
        self.table_index_pages = os.getenv('BASE_TABLE_INDEX_PAGES')
        self.table_unique_visitors = os.getenv('BASE_TABLE_UNIQUE_VISITORS')
        self.table_sitemap_and_robots = os.getenv('BASE_TABLE_SITEMAP_AND_ROBOTS')


# Общие настройки подключения к рекламным кабинетам
class Settings:

    def __init__(self):

        ads_cabinets = os.getenv('AD_CABINETS')
        ads_cabinets.replace('\n', '').replace('\"', '"')
        self.ads_cabinets = json.loads(ads_cabinets, strict=False)

    def get_period(self):
        db_set = DatabaseSet()
        with Database(db_set) as db:
            try:
                values_list = db.table_spent.col_values(2)
                self.start_date = values_list[-1]
                self.start_date = datetime.strptime(self.start_date, STRING_DATE_FORMAT).date()
                self.start_date = self.start_date + timedelta(days=1)  # Следующий день после последней загрузки
            except IndexError:
                self.start_date = input('Введите дату, с которой необходимо начать сбор данных '
                                        f'в формате {INPUT_DATE_FORMAT}: ')
                self.start_date = datetime.strptime(self.start_date, STRING_DATE_FORMAT).date()  # Ручной ввод даты

        self.end_date = date.today() - timedelta(days=1)  # Окончание периода - вчера
        date_diff = (self.end_date - self.start_date).days

        self.start_date, self.end_date = tuple(map(str, [self.start_date, self.end_date]))

        # Проверка, запускался ли скрипт в текущие сутки; если нет, датам присваивается None
        if date_diff < 0:
            self.start_date, self.end_date = None, None

        return self


# Настройки подключения к кабинетам Вконтакте
class VkSet(DatabaseSet):
    def __init__(self, **kwargs):

        DatabaseSet.__init__(self)
        self.system = 'vk'
        self.api_ver = os.getenv('VK_API_VER')
        self.app_id = os.getenv('VK_APP_ID')
        self.app_secret = os.getenv('VK_APP_SECRET')
        self.redirect = os.getenv('VK_REDIRECT')
        self.user_id = os.getenv('VK_USER_ID')
        self.token = os.getenv('VK_TOKEN')
        self.account_id = int(kwargs.get('account_id'))
        self.executor = kwargs.get('executor')
        self.client = kwargs.get('client', None)
        self.start_date = kwargs.get('start_date')
        self.end_date = kwargs.get('end_date')
        self.client_id = int(kwargs.get('client_id')) if kwargs.get('client_id', None) is not None else None


# Настройки подключения к кабинетам Яндекс Директ
class DirektSet(Settings, DatabaseSet):
    def __init__(self, **kwargs):

        DatabaseSet.__init__(self)
        self.get_period()
        self.system = 'yandex'
        self.account_name = kwargs.get('account_name')
        self.token = kwargs.get('token')
        self.start_date = kwargs.get('start_date')
        self.end_date = kwargs.get('end_date')


class Connector:

    def __init__(self, settings):
        self.settings = settings
        # self.all_data = None

    @flood_error_proceed
    def get_request(self, url, body=None, headers=None):
        result = requests.get(url, headers=headers) if body is None else requests.get(url, body, headers=headers)
        result_temp = result.json()

        if result_temp.get('error') is not None:
            if result_temp['error']['error_code'] == 9 or result_temp['error']['error_code'] == 6:
                raise FloodError(result_temp['error']['error_msg'])
        else:
            return result

    def parse_result(self, request, key_name='id', value_name='name') -> list:
        result = {}
        for item in request:
            try:
                try:
                    result[item.get(key_name)] = self._to_float(item.get(value_name))
                except ValueError:
                    result[item.get(key_name)] = item.get(value_name)
            except KeyError:
                result[item.get(key_name)] = 0
        return result

    def prepare_result_to_db(self, data, key_name='id', value_name='name'):
        result = self.parse_result(data, key_name, value_name)
        for item in result:
            item.insert(0, self.client_id)
            item.extend([self.utm_source, self.utm_medium, self.system, self.executor, self.traffic_type])
        return result

    def prepare_balance_to_db(self, balance):
        data = []
        date = datetime.now().strftime('%Y-%m-%d')
        balance_upd = float(balance)

        data.extend([self.client_id, date, balance_upd])
        return data

    def check_result(self, result: dict):
        for k, v in result.items():
            print(k, v)

    def check_to_excel(self, data, file_name):
        df = pd.DataFrame(data)
        df.to_excel(file_name, index=False)

    def check_from_excel(self, file_name, sheet_name):
        return pd.read_excel(file_name, sheet_name=sheet_name)

    def update_base(self):
        pass

    def set_client_id(self, client_id):
        self.client_id = client_id
        return self

    def set_utm_source(self, utm_source):
        self.utm_source = utm_source
        return self

    def set_utm_medium(self, utm_medium):
        self.utm_medium = utm_medium
        return self

    def set_system(self, system):
        self.system = system
        return self

    def set_executor(self, executor):
        self.executor = executor
        return self

    def set_traffic_type(self, traffic_type):
        self.traffic_type = traffic_type
        return self

    @staticmethod
    def _to_float(value):
        try:
            return round(float(value), 2)
        except TypeError:
            return 0

    def _filter_data(self, plan):
        # Set needed filters
        date_filters = (self.all_data['date_create'] >= plan['start_date']) \
                       & (self.all_data['date_create'] <= plan['end_date'])
        other_filters = (self.all_data['executor'] == plan['executor']) \
                        & (self.all_data['category_name'] == plan['category_name']) \
                        & (self.all_data['stage_type'] != 'Тестирование')

        # ОСТОРОЖНО, КОСТЫЛЬ!
        if plan['executor'] == 'Стенд на МК':
            other_filters = (self.all_data['executor'] == plan['executor']) \
                            & (self.all_data['stage_type'] != 'Тестирование')

        # Filter dataframe
        temp_df = self.all_data.loc[date_filters].loc[other_filters]

        # Return result
        return temp_df

    def set_plans(self, plan):
        self.plan = plan
        return self

    @staticmethod
    def _current_day_count(plan):
        today = date.today()
        finish_dt = datetime.strptime(plan['end_date'], '%Y-%m-%d')

        in_month = calendar.monthrange(finish_dt.year, finish_dt.month)[1]
        from_start = today.day - 1 if today.month == finish_dt.month else in_month

        Days = namedtuple('Dates', ['in_month', 'from_start'])
        return Days(in_month, from_start)


class Vk(Connector):

    def __init__(self, settings):
        super().__init__(settings)
        self.token = self.settings.token  # Will be in future set by get_token method

    def get_token(self):
        # Authorize flow
        url = 'https://oauth.vk.com/authorize'
        body = {
            'client_id': self.settings.app_id,
            'redirect_uri': self.settings.redirect,
            'scope': 'ads',
            'response_type': 'code'
        }

        result = self.get_request(url, body)
        print(result.status_code, result.text)

        # Get token flow
        url = 'https://oauth.vk.com/access_token'
        body = {
            'client_id': self.settings.app_id,
            'client_secret': self.settings.app_secret,
            'redirect_uri': self.settings.redirect,
            'scope': 'ads',
            'response_type': 'code'
        }

        # Here should be token variable update
        pass

    def get_accounts(self):
        url = 'https://api.vk.com/method/ads.getAccounts'
        body = {
            'access_token': self.token,
            'v': self.settings.api_ver
        }

        result = self.get_request(url, body).json()['response']
        key_name = 'account_id'
        value_name = 'account_name'
        result_dict = self.parse_result(result, key_name, value_name)
        return result_dict

    def get_clients(self):
        url = 'https://api.vk.com/method/ads.getClients'
        body = {
            'account_id': self.settings.account_id,
            'access_token': self.token,
            'v': self.settings.api_ver
        }
        try:
            result = self.get_request(url, body).json()['response']
            result_dict = self.parse_result(result)
            return result_dict
        except KeyError:
            result = self.get_request(url, body).json()
            print(result)

    def get_campaigns(self):
        url = 'https://api.vk.com/method/ads.getCampaigns'
        body = {
            'account_id': self.settings.account_id,
            'client_id': self.settings.client_id,
            'include_deleted': 0,
            'access_token': self.token,
            'v': self.settings.api_ver
        }
        result = self.get_request(url, body).json()['response']

        result_dict = self.parse_result(result)
        return result_dict

    def get_report(self):
        url = 'https://api.vk.com/method/ads.getStatistics'
        body = {
            'account_id': self.settings.account_id,
            'period': 'day',
            'date_from': self.settings.start_date,
            'date_to': self.settings.end_date,
            'access_token': self.token,
            'v': self.settings.api_ver
        }

        if self.settings.client_id is not None:
            body['ids_type'] = 'client'
            body['ids'] = self.settings.client_id
        elif self.settings.account_id is not None:
            body['ids_type'] = 'office'
            body['ids'] = self.settings.account_id

        result = self.get_request(url, body).json()['response'][0]['stats']

        key_name = 'day'
        value_name = 'spent'
        self.all_data = self.prepare_result_to_db(result, key_name=key_name, value_name=value_name)
        return self.all_data

    def get_balance(self):
        url = 'https://api.vk.com/method/ads.getBudget'
        if self.settings.client_id is not None:
            result_new = None
        else:
            body = {
                'account_id': self.settings.account_id,
                'access_token': self.token,
                'v': self.settings.api_ver
            }
            result = self.get_request(url, body).json()
            result = result['response']

            result_new = self.prepare_balance_to_db(result)
        return result_new

    @property
    def get_limit(self):
        url = 'https://api.vk.com/method/ads.getClients'
        body = {
            'account_id': self.settings.account_id,
            'access_token': self.token,
            'v': self.settings.api_ver
        }
        result_raw = self.get_request(url, body).json()['response']
        value_name = 'day_limit'
        result = self.parse_result(result_raw, value_name=value_name)

        return [item for item in result if item[0] == self.settings.client_id]

    def set_plans(self, plans, plan_metric=None, col_name=None):
        self.all_data = pd.DataFrame(self.all_data)
        Connector.set_plans(self, plans, plan_metric=None, col_name=None)


class Direkt(Connector):

    def __init__(self, settings):
        super().__init__(settings)

    def parse_result(self, result, key_name='id', value_name='name') -> list:
        values = result.split('\n')
        values_new = []
        for item in values:
            value = item.split('\t')
            if len(value) > 1:
                try:
                    value[1] = self._to_float(value[1])
                except ValueError:
                    pass
                values_new.append(value)
        del values_new[0]
        return values_new

    # @protected
    def get_request_api5(self, url, body=None, headers=None):
        while True:
            req = requests.post(url, body, headers=headers)
            req.encoding = 'utf-8'
            if req.status_code == 200:
                logger.info('Отчет создан успешно')
                return req.text
                # return req
            elif req.status_code == 201:
                logger.info('Отчет успешно поставлен в очередь в режиме офлайн')
                retryIn = int(req.headers.get("retryIn", 60))
                logger.info(f'Повторная отправка запроса через {retryIn} секунд')
                sleep(retryIn)
            elif req.status_code == 202:
                logger.info('Отчет формируется в режиме офлайн')
                retryIn = int(req.headers.get("retryIn", 60))
                logger.info(f'Повторная отправка запроса через {retryIn} секунд')
                sleep(retryIn)
            else:
                logger.warning('Произошла ошибка')
                logger.warning(f'Код ответа {req.status_code}')
                logger.warning(f'JSON-код запроса: {body}')
                logger.warning(f'JSON-код ответа сервера: \n{req.json()}')
                break

    # @protected
    def get_request_api4(self, url, body):
        response = requests.post(url, json=body)  # Try to use json.dumps; thus parent's method can be used
        return response

    def get_report(self):
        field_names = ["Date", "Cost"]
        report_name = datetime.now().timestamp()

        url = 'https://api.direct.yandex.com/json/v5/reports'
        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": self.settings.start_date,
                    "DateTo": self.settings.end_date
                },
                "FieldNames": field_names,
                "ReportName": f"REPORT_{report_name}",
                "ReportType": "CUSTOM_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        body = json.dumps(body, indent=4)
        headers = {
            "Authorization": "Bearer " + self.settings.token,
            "Accept-Language": "ru",
            "processingMode": "offline",
            "returnMoneyInMicros": "false",
            "skipReportHeader": "true",
            "skipReportSummary": "true"
        }
        if self.settings.account_name is not None:
            headers['Client-Login'] = self.settings.account_name

        result_raw = self.get_request_api5(url, body, headers)
        self.all_data = self.prepare_result_to_db(result_raw)
        return self.all_data

    def get_balance(self):
        url = 'https://api.direct.yandex.ru/live/v4/json/'
        body = {
            "method": "AccountManagement",
            "param": {
                "Action": "Get",
                "Logins": [self.settings.account_name],
            },
            "token": self.settings.token
        }

        result_raw = self.get_request_api4(url, body)
        result_json = result_raw.json()['data']['Accounts'][0]
        result = result_json["Amount"]
        result_new = self.prepare_balance_to_db(result)
        return result_new

    def set_plans(self, plans, plan_metric=None, col_name=None):
        self.all_data = pd.DataFrame(self.all_data)
        Connector.set_plans(self, plans, plan_metric=None, col_name=None)

    def check_balls(self):
        url = 'https://api.direct.yandex.com/json/v5/reports'
        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": self.settings.start_date,
                    "DateTo": self.settings.end_date
                },
                "FieldNames": ["Date", "Cost"],
                "ReportName": f"REPORT",
                "ReportType": "CUSTOM_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        body = json.dumps(body, indent=4)
        headers = {
            "Authorization": "Bearer " + self.settings.token,
            "Accept-Language": "ru",
            "processingMode": "offline",
            "returnMoneyInMicros": "false",
            "skipReportHeader": "true",
            "skipReportSummary": "true",
            # "Use-Operator-Units": "true",
        }
        if self.settings.account_name is not None:
            headers['Client-Login'] = self.settings.account_name

        result_raw = self.get_request_api5(url, body, headers)
        res = result_raw.headers
        return res


class Manual(Connector):

    def __init__(self, settings):
        super().__init__(settings)
        self.settings = settings
        self.updated_rows = []
        self.y_massive = []
        self.clients = []
        self.raw_values = []
        self.values_to_upload = []
        self.executor = None
        self.traffic_type = None

    def __enter__(self):
        self.gs = gs.service_account_from_dict(self.settings.key)
        self.sheet = self.gs.open_by_key(self.settings.sheet_name)

        self.table_clients = self.sheet.worksheet(self.settings.table_clients)
        self.table_balances_vk = self.sheet.worksheet(self.settings.table_balances_vk)
        self.table_spent = self.sheet.worksheet(self.settings.table_spent)
        self.table_plan = self.sheet.worksheet(self.settings.table_plan)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @staticmethod
    def _convert_date(date):
        return datetime.strptime(date, '%Y-%m-%d').date()

    @staticmethod
    def _get_range(table, new_names, mark_row):
        all_values_count = table.col_values(1)
        last_row = len(all_values_count)

        if mark_row is None:
            first_row = 4
            col_int = len(new_names) + 64
            mark_row = chr(col_int)
        else:
            mark_row_int = ord(mark_row) - 64
            mark_row_values = table.col_values(mark_row_int)
            first_row = len(mark_row_values) + 1

        range_cells = f'A{first_row}:{mark_row}{last_row}' if first_row-1 != last_row else None
        return range_cells, first_row

    def get_values(self, table, new_names, mark_row=None) -> list:
        range_cells, first_row = self._get_range(table, new_names, mark_row)

        raw_values = []
        if range_cells is not None:
            raw_values_list = table.get(range_cells)

            assert len(raw_values_list[0]) == len(new_names)
            assert self.updated_rows == []
            assert self.y_massive == []

            for num, row in enumerate(raw_values_list):
                dictionary = dict(zip(new_names, row))
                raw_values.append(dictionary)
                self.updated_rows.append(num + first_row)
                self.y_massive.append(['Y'])

            if mark_row is not None:
                table.update(f'{mark_row}{self.updated_rows[0]}:{mark_row}{self.updated_rows[-1]}', self.y_massive)
            self.updated_rows = []
            self.y_massive = []

        return raw_values

    def match_utms(self):
        """Find utm_source and utm_medium from clients table"""
        for item in self.raw_values:
            for client in self.clients:
                if item['executor'] == client['executor'] and item['system'] == client['system']:
                    item['utm_source'] = client['utm_source']
                    item['utm_medium'] = client['utm_medium']
                    break

            assert item.get('utm_source') is not None
            assert item.get('utm_medium') is not None
            del item['system']
        return self

    def set_type(self, item):

        if item['utm_source'] is None and item['utm_source'] is None:
            self.temp_result['traffic_type'] = 'Бесплатный'
        else:
            for i in self.clients_info:
                if i['utm_source'] == self.temp_result['utm_source'] and i['utm_medium'] == self.temp_result['utm_medium']:
                    self.temp_result['executor'] = i['executor']
                    self.temp_result['traffic_type'] = i['traffic_type']
                    break

        if self.temp_result['traffic_type'] is None:
            self.temp_result['traffic_type'] = 'Условно-бесплатный'

        return self

    def _dates_to_date(self, item):
        start_date = self._convert_date(item['start_date'])
        end_date = self._convert_date(item['end_date'])
        return start_date, end_date

    def split_dates(self, split_case):
        """Split one row to many if there are more than 1 day between"""
        for item in self.raw_values:
            start_date, end_date = self._dates_to_date(item)
            days_count = (end_date - start_date).days + 1
            if days_count > 1:
                dates = [start_date]
                while start_date < end_date:
                    start_date += timedelta(days=1)
                    dates.append(start_date)
                dates.append(end_date)

                if split_case == 'plans':
                    new_items = [
                        [dates[k].strftime('%Y-%m-%d'),  # date
                        item['category_name'],  # category_name
                        item['utm_source'],  # utm_source
                        item['utm_medium'],  # utm_medium
                        round((float(item['leads']) / days_count), 2),  # leads_count
                        round((float(item['budjet']) / days_count), 2),  # budjet
                        item['executor'],
                         ] for k in range(days_count)
                    ]
                    self.values_to_upload.extend(new_items)
                elif split_case == 'spent':
                    new_items = [
                        ['-',  # client_id = None for spents, added manually
                         dates[k].strftime('%Y-%m-%d'),  # date
                         round((float(item['spent']) / days_count), 2),  # spent
                         item['utm_source'],  # utm_source
                         item['utm_medium'],  # utm_medium
                         ] for k in range(days_count)
                    ]
                    self.values_to_upload.extend(new_items)
            else:
                self.values_to_upload.append(item)
        return self.values_to_upload

    def get_clients(self):
        table = self.table_clients
        new_names = ['utm_source', 'utm_medium', 'executor', 'system', 'account_name', 'client_name', 'traffic_type']
        self.clients = self.get_values(table, new_names)

    def get_plans(self):
        table = self.table_plan
        new_names = ['start_date', 'end_date', 'executor', 'category_name', 'system', 'plan_leads', 'budjet', 'cpl']
        self.raw_values = self.get_values(table,
                                          new_names,
                                          )
        return self

    def get_manual_spent(self):
        table = self.table_spent
        new_names = ['start_date', 'end_date', 'utm_source', 'utm_medium', 'spent']
        mark_row = 'F'
        self.raw_values = self.get_values(table, new_names, mark_row)

        result = self.split_dates('spent')
        self.raw_values, self.values_to_upload = [], []
        return result

    def get_vk_balances(self, vk_ids, vk_spent):
        table = self.table_balances_vk
        new_names = ['account_name', 'client_name', 'date', 'money']
        self.raw_values = self.get_values(table, new_names)

        # Drop date atribute (this should be removed later)
        for item in self.raw_values:
            del item['date']

        # Sum all money of the same accounts
        values = pd.DataFrame(self.raw_values)
        values_grouped = values.groupby(by=['account_name', 'client_name']).sum().todict()

        # Create a dict with vk money
        money_vk = {}
        for item in values_grouped:
            for vk in vk_ids:
                if item['account_name'] == vk['account_name'] and item['client_name'] == vk['client_name']:
                    item['id'] = vk['client_base_id']
                    money_vk[vk['id']] = item['money']

        spent = pd.DataFrame(vk_spent)
        money = pd.DataFrame(money_vk)

        balances = money.merge(spent, how='inner', on='id')


if __name__ == '__main__':
    pass
