import logging

import gspread
from gspread import exceptions

import os
import json
import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)


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


class Database:
    """Класс для подключения к Google таблицам"""

    def __init__(self, settings):
        self.settings = settings
        self.clients = []
        self.sheets = None

    def __enter__(self):
        self.gs = gspread.service_account_from_dict(self.settings.key)
        self.sheet = self.gs.open_by_key(self.settings.sheet_name)

        self.table_clients = self.sheet.worksheet(self.settings.table_clients)
        self.table_balances = self.sheet.worksheet(self.settings.table_balances)
        self.table_spent = self.sheet.worksheet(self.settings.table_spent)
        self.table_crm = self.sheet.worksheet(self.settings.table_crm)
        self.table_plan = self.sheet.worksheet(self.settings.table_plan)
        self.table_deals_crm = self.sheet.worksheet(self.settings.table_deals_crm)
        self.table_index_pages = self.sheet.worksheet(self.settings.table_index_pages)
        self.table_unique_visitors = self.sheet.worksheet(self.settings.table_unique_visitors)
        self.table_sitemap_and_robots = self.sheet.worksheet(self.settings.table_sitemap_and_robots)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_worksheets(self):
        self.sheets = self.sheet.worksheets()
        return self.sheets

    def get_client_data(self):
        """Забирает и классифицирует данные из рекламного кабинета"""

        all_values = self.table_clients.get_all_records()
        for row in all_values:

            if self.settings.system == row['system'] == 'vk':
                if self.settings.client_id is not None and row['client_id'] != '':
                    if int(self.settings.client_id) == row['client_id']:
                        self.client_id = row['id']
                        self.utm_source = row['utm_source']
                        self.utm_medium = row['utm_medium']
                        self.system = row['system']
                        self.executor = row['executor']
                        self.traffic_type = row['traffic_type']
                        break
                else:
                    if int(self.settings.account_id) == row['account_id']:
                        self.client_id = row['id']
                        self.utm_source = row['utm_source']
                        self.utm_medium = row['utm_medium']
                        self.system = row['system']
                        self.executor = row['executor']
                        self.traffic_type = row['traffic_type']
                        break

            elif self.settings.system == row['system'] == 'yandex':
                if self.settings.account_name == row['account_name']:
                    self.client_id = row['id']
                    self.utm_source = row['utm_source']
                    self.utm_medium = row['utm_medium']
                    self.system = row['system']
                    self.executor = row['executor']
                    self.traffic_type = row['traffic_type']
                    break
        return self

    def match_utms(self):
        all_values = self.table_clients.get_all_records()
        for row in all_values:
            temp_data = {
                'utm_source': row['utm_source'],
                'utm_medium': row['utm_medium'],
                'executor': row['executor'],
                'traffic_type': row['traffic_type'],
                'system': row['system'],
            }
            self.clients.append(temp_data)

    def get_vk_ids(self) -> list:
        all_values = self.table_clients.get_all_records()
        vk_ids = []
        for row in all_values:
            if row['system'] == 'vk' and row.get('client_id') is not None:
                temp_data = {
                    'account_name': row['account_name'],
                    'client_name': row['client_name'],
                    'client_base_id': row['id'],
                }
                vk_ids.append(temp_data)
        return vk_ids

    # Need also to check dates here
    def get_vk_spent(self, vk_ids: list) -> dict:
        all_values = self.table_spent.get_all_records()
        vk_ids_list = [i['client_base_id'] for i in vk_ids]

        vk_spent = {}
        for row in all_values:
            for vk_id in vk_ids_list:
                vk_id_spent = 0
                if row['id'] == vk_id:
                    vk_id_spent += int(row['spent'])
                vk_spent[vk_id] = vk_id_spent

        return vk_spent

    def upload_spent(self, data):
        self.table_spent.append_rows(data)

    def upload_balance(self, data):
        all_values = self.table_balances.get_all_records()
        is_added = False
        for num, row in enumerate(all_values):
            if self.client_id == row['client_id']:
                row_number = num+2
                self.table_balances.update(f'A{row_number}:C{row_number}', [data])
                is_added = True
                break
        if not is_added:
            self.table_balances.append_row(data)

    def upload_crm(self, data):

        self._get_first_cell(data)
        self._delete_data()
        self.table_crm.append_rows(data.values.tolist())

    def upload_deals_crm(self, data):

        self._get_first_cell(data)
        self._delete_data_deals()
        self.table_deals_crm.append_rows(data.values.tolist())

    def upload_index_pages(self, data):
        self.table_index_pages.clear()
        self.table_index_pages.append_rows(data.values.tolist())

    def upload_unique_visitors(self, data):
        self.table_unique_visitors.clear()
        self.table_unique_visitors.append_rows(data.values.tolist())

    def upload_sitemap_and_robots(self, data):
        self.table_sitemap_and_robots.clear()
        self.table_sitemap_and_robots.append_rows(data)

    def _delete_data_deals(self):
        # Deletes all values, starting from last which is in new file
        last_value_cell = len(self.table_deals_crm.col_values(1)) + 1
        try:
            first_value_cell = self.table_deals_crm.find(str(self.first_cell)).row
        except AttributeError:
            first_value_cell = last_value_cell

        try:
            self.table_deals_crm.delete_rows(first_value_cell, last_value_cell)
        except exceptions.APIError:
            pass

    def _get_first_cell(self, data):
        temp_data = data.values.tolist()
        self.first_cell = temp_data[0][0]

    def _delete_data(self):
        # Deletes all values, starting from last which is in new file
        last_value_cell = len(self.table_crm.col_values(1)) + 1
        try:
            first_value_cell = self.table_crm.find(str(self.first_cell)).row
        except AttributeError:
            first_value_cell = last_value_cell

        try:
            self.table_crm.delete_rows(first_value_cell, last_value_cell)
        except exceptions.APIError:
            pass

    def upload_plans(self, data):
        self.table_plan.append_rows(data)


if __name__ == '__main__':
    pass
