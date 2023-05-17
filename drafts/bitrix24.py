import logging

import pandas as pd
import numpy as np
from bitrix24 import Bitrix24

import src.config.bitrix_config as config
from src.entities.connect import Connector
from src.config import config as c
from src.config import templates as t


logger = logging.getLogger(__name__)


class BitrixFactory:
    """Фабрика для создания объекта конфигурации и отчета"""

    def __init__(self, entity):
        """
        Возвращает объекты настроек и отчета в зависимости от переданной метки.
        :param entity: Метка типа объектов, которые ожидаются на выходе.
            Доступные значения:
            lead: вернет объекты BitrixLeadSet и BitrixLeadReport;
            deal: вернет объекты BitrixDealSet и BitrixDealReport.
        """

        self.entity = entity
        self.loader, self.report = None, None
        self._choose()

    def _choose(self):
        """Выбирает необходимый объект"""

        match self.entity:
            case 'lead':
                settings = config.BitrixLeadSet()
                self.report = BitrixLeadReport(settings)
            case 'deal':
                settings = config.BitrixDealSet()
                self.report = BitrixDealReport(settings)
            case _:
                raise NotImplementedError('Please use one of supported entities')

        self.loader = BitrixLoader(settings)

    def get(self):
        """Возвращает объкты настроек"""
        return self.loader, self.report


class BitrixLoader(Bitrix24, Connector):
    """Класс для подключения к Битрикс24 и загрузки данных"""

    def __init__(self, settings):
        self.settings = settings
        Bitrix24.__init__(self, self.settings.url)
        self.raw_data = None
        self.data = None

    def _get_raw_data(self):
        """Возвращает сырые данные отчета из Битрикс24"""
        fields = list(self.settings.fields_map.keys())

        self.raw_data = self.callMethod(self.settings.method,
                                        filter=self.settings.filters,
                                        select=fields)

    def _convert_to_df(self):
        """Перекладывает сырые данные в DataFrame"""
        self.data = pd.DataFrame(self.raw_data)

    def get(self):
        self._get_raw_data()
        self._convert_to_df()
        return self.data

    def get_sources(self):
        """Получает все источники лидов, заведенные в Битрикс24"""
        method = 'crm.status.list'
        filter = {
            'ENTITY_ID': 'SOURCE'
        }
        result = self.callMethod(method, filter=filter)
        result_new = self.parse_result(result, key_name='STATUS_ID', value_name='NAME')
        return result_new


class BitrixReport:
    """Сущность отчета из Битрикс24"""
    _abstract = True

    def __init__(self, settings):
        if self._abstract:
            raise NotImplementedError('Please try to specify settings')

        self.data, self.mapping = None, None
        self.settings = settings

    def add(self, data):
        """Добавляет данные для работы к объекту"""
        self.data = data

    def get(self):
        """Возвращает данные отчета"""

        self._replace_nan()
        return self.data

    def clean(self):
        """Очищает и нормализует данные отчета"""
        self._rename()
        logger.debug(f'After _rename: {len(self.data)}')

        self._source()
        logger.debug(f'After _source: {len(self.data)}')

        self._date()
        logger.debug(f'After _date: {len(self.data)}')

        self._source_description()
        logger.debug(f'After _source_description: {len(self.data)}')

        self._hr_services()
        logger.debug(f'After _hr_services: {len(self.data)}')

        self._status()
        logger.debug(f'After _status: {len(self.data)}')

    def get_divided(self):
        """Возвращает данные отчета в виде словаря, разделенные на группы для последующего объединения"""

        all_utms = self.data.loc[(self.data['utm_source'].notna()) & (self.data['utm_medium'].notna())]
        only_source = self.data.loc[(self.data['utm_source'].notna()) & (self.data['utm_medium'].isna())]
        other = pd.concat([self.data, all_utms, only_source]).drop_duplicates(keep=False)

        divided_data = t.MapTemplate()
        divided_data.add('all_utms', all_utms)
        divided_data.add('only_source', only_source)
        divided_data.add('other', other)

        return divided_data.get()

    def _rename(self):
        """
        Приводит имена полей к единому виду,
        используя маппинг из self.settings.fields_map
        """
        self.data.rename(columns=self.settings.fields_map, inplace=True)

    def _replace_nan(self):
        """Заменяет nan на None по всему DataFrame"""
        self.data.replace(np.nan, None, inplace=True)

    def _source(self):
        """Подбирает источник по его ID"""
        self.data = self.data.merge(self.settings.needed_sources, how='left', on='source_id')
        self.data.drop(columns='source_id', inplace=True)

    def _hr_services(self):
        """
        ВСЕЛЕНСКОГО РАЗМЕРА КОСТЫЛЬ
        if category == 'Курсы' && '/hr-recruitment' in source_description:
            category == 'HR Services'

        Зависит от маппинга в self._source() и очистки описания источника в self._source_description()
        """
        self.data['category'] = self.data.apply(
            lambda row: self._clean_hr_services(row),
            axis=1
        )

    @staticmethod
    def _clean_hr_services(row):
        """
        Делает всю грязную работу для метода self._hr_services()
        """

        temp_cat = row['category']
        temp_desc = row['source_description']

        try:
            if '/hr-recruitment' in temp_desc and temp_cat == 'Курсы':
                temp_cat = 'HR Services'
        except TypeError:
            pass

        return temp_cat

    def _date(self):
        """Оставляет только дату из строки с датой и временем"""

        # TODO: извлечь только дату из даты и времени с помощью np.datetime

        self.data['date'] = self.data.apply(
            lambda row: self._slice_date(row),
            axis=1
        )

    @staticmethod
    def _slice_date(row):
        return row['date'][:10]

    def _source_description(self):

        # Очищаем данные по источнику
        self.data['source_description'] = self.data.apply(
            lambda row: self._clean_source_description(row),
            axis=1
        )

        # Удаляем из выборки лиды по звонкам
        term = (self.data['source_description'] != 'Звонок поступил на номер: Приложение: MANGO OFFICE Виртуальная АТС.')
        self.data = self.data.loc[term]

    @staticmethod
    def _clean_source_description(row):
        temp = row['source_description']

        if temp:
            temp = temp[8:] if temp.startswith('https://') else temp
            temp = temp[:-1] if temp[-1] == '/' else temp

            signs_to_split = ['#', '?']
            for sign in signs_to_split:
                if sign in temp:
                    temp = temp.split(sign, maxsplit=1)[0]

        return temp

    def _status(self):
        """Подбирает группу статуса по его ID"""
        self.data = self.data.merge(self.settings.status_types_df, how='left', on='status_id')
        self.data.drop(columns='status_id', inplace=True)


class BitrixLeadReport(BitrixReport):
    """Класс для представления отчета по лидам из Битрикс24"""
    _abstract = False

    def clean(self):
        BitrixReport.clean(self)
        self._filter_by_assignee()
        logger.debug(f'After _filter_by_assignee: {len(self.data)}')

    def get(self):
        """Возвращает данные отчета в том порядке полей, которые добавлены в таблице лидов"""
        self._replace_nan()
        return self.data[self.settings.columns_order]

    def _filter_by_assignee(self):

        # TODO: придумать как прописать условия красиво, а не перебором

        term1 = (self.data['category'] == 'Курсы') & (self.data['assigned_by_id'].isin(['20', '3680', '3024']))
        term2 = (self.data['category'] != 'Курсы')

        self.data = self.data.loc[
            term1 | term2
        ]

        self.data.drop(columns='assigned_by_id', inplace=True)


class BitrixDealReport(BitrixReport):
    """Класс для представления отчета по сделкам из Битрикс24"""
    _abstract = False

    def clean(self):
        BitrixReport.clean(self)
        self._revenue()

    def _revenue(self):
        """Переводит сумму сделки в числовой тип"""

        self.data['revenue'] = pd.to_numeric(self.data['revenue'])


if __name__ == '__main__':
    db_set = c.DatabaseSet()  # Задаем настройки подключения к google таблицам

    factory = BitrixFactory('lead')  # Создаем объект фабрики
    loader, report = factory.get()  # Получаем объекты загрузчика и отчета

    raw_data = loader.get()  # Получаем данные отчета из Б24
    report.add(raw_data)  # Добавляем данные отчета к объекту отчета
    report.clean()  # Очищаем и преобразуем данные отчета

    report_divided_dict = report.get_divided()
