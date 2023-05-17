import os
import json
import logging
from datetime import date, timedelta

import pandas as pd

import src.config.config as c

# from dotenv import load_dotenv
# load_dotenv()

logger = logging.getLogger(__name__)


class BitrixSettings:
    """Общие настройки подключения к Битрикс24"""
    _abstract = True

    def __init__(self):
        if self._abstract:
            raise NotImplementedError('Please try to specify settings')

        token = f'/{os.getenv("BX_TOKEN")}/'
        domain = os.getenv("BX_DOMAIN")
        url = [f'https://{domain}/rest', token]
        self.url = ''.join(url)

        self.status_types = None
        self.method = None

        self._get_period()
        self._get_sources()

        # Словарь с указанием ID сущностей в Битрикс; перечислены только категории сделок
        self.needed_categories = {
            'Курс': 2,
            'Вебинар': 68,
        }
        self.category_ids = list(self.needed_categories.values())

        # Словарь, содержащий данные об ответственных по каждому направлению
        self.needed_assignee_id = {
            'Курсы': [20, 3680, 3024]
        }

    def _get_period(self):
        """Получает необходимый период для загрузки данных"""

        days_delta = int(os.getenv('BX_WINDOW'))  # Окно загрузки данных
        first_date = date.today() - timedelta(days=days_delta)
        self.start_date = date(first_date.year, first_date.month, 1).strftime(c.BX_DATE_FORMAT)
        self.end_date = date.today().strftime(c.BX_DATE_FORMAT)

    def _get_sources(self):
        """Получает словарь с необходимыми источниками из env и формирует DataFrame"""

        needed_sources = os.getenv('NEEDED_SOURCES')
        needed_sources = json.loads(needed_sources, strict=False)

        self.needed_sources = []

        for k, v in needed_sources.items():
            temp_source_ids = list(v.keys())

            for num, s in enumerate(temp_source_ids):
                temp = {
                    'category': k,
                    'source_id': s,
                    'source_name': v.get(s)
                }
                self.needed_sources.append(temp)

        self.needed_sources = pd.DataFrame(self.needed_sources)
        self.sources = list(self.needed_sources['source_id'])

    def _status_types_to_df(self):
        """Перекладывает словарь с типами статусов в DataFrame"""

        self.status_types_df = []

        for k, v in self.status_types.items():
            temp_status_types = list(v.keys())

            for s in temp_status_types:
                temp = {
                    'status_type': k,
                    'status_id': s,
                }
                self.status_types_df.append(temp)

        self.status_types_df = pd.DataFrame(self.status_types_df)


class BitrixLeadSet(BitrixSettings):
    """Настройки подключения к Битрикс24 для загрузки лидов"""
    _abstract = False

    def __init__(self):
        BitrixSettings.__init__(self)

        self.method = 'crm.lead.list'

        self.status_types = {
            'В работе': {
                'NEW': 'Не обработан',
            },
            'Успех': {
                'CONVERTED': 'Заинтересованный клиент',
            },
            'Провал': {
                'JUNK': 'Я не дозвонился',
            },
            'Тестирование': {
                '2': 'Дубль',
                '28': 'Тестирование',
            },
            'Нецелевой': {
                '30': 'Нецелевой запрос',
            },
        }
        self._status_types_to_df()

        self.filters = {
            '>DATE_CREATE': self.start_date,
            '<DATE_CREATE': self.end_date,
            '%SOURCE_ID': self.sources
        }

        self.fields_map = {
            'ID': 'id',
            'DATE_CREATE': 'date',
            'SOURCE_ID': 'source_id',
            'SOURCE_DESCRIPTION': 'source_description',
            'STATUS_ID': 'status_id',
            'ASSIGNED_BY_ID': 'assigned_by_id',
            'UTM_SOURCE': 'utm_source',
            'UTM_MEDIUM': 'utm_medium',
            'UTM_CAMPAIGN': 'utm_campaign',
            # 'TITLE': 'title',
        }

        self.columns_order = ['id', 'utm_source', 'utm_medium', 'utm_campaign', 'date',
                              'source_description', 'status_type', 'category', 'source_name', 'executor',
                              'traffic_type', 'system',
                              # 'title'
                              ]


class BitrixDealSet(BitrixSettings):
    """Настройки подключения к Битрикс24 для загрузки сделок"""
    _abstract = False

    def __init__(self):
        BitrixSettings.__init__(self)

        self.method = 'crm.deal.list'

        self.status_types = {
            'В работе': {
                'C2:NEW': 'Новая',
            },
            'Успех': {
                'C2:7': 'Оплачена онлайн',
            },
            'Провал': {
                'DT31_2:D': 'Не оплачен',
            },
            'Тестирование': {
                'C2:APOLOGY': 'Тестовая',
            },
        }
        self._status_types_to_df()

        self.filters = {
            '>DATE_CREATE': self.start_date,
            '<DATE_CREATE': self.end_date,
            '%SOURCE_ID': self.sources,
            '%CATEGORY_ID': self.category_ids
        }

        self.fields_map = {
            'ID': 'id',
            'DATE_CREATE': 'date',
            'SOURCE_ID': 'source_id',
            'SOURCE_DESCRIPTION': 'source_description',
            'STAGE_ID': 'status_id',
            'UTM_SOURCE': 'utm_source',
            'UTM_MEDIUM': 'utm_medium',
            'UTM_CAMPAIGN': 'utm_campaign',
            'LEAD_ID': 'connected_lead_id',
            'OPPORTUNITY': 'revenue',
        }
