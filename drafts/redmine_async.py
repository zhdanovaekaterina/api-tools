import os
import asyncio
import logging
import time as t
from abc import ABC, abstractmethod
from pprint import pprint

import pandas as pd
from dotenv import load_dotenv
from aiohttp import ClientSession

logger = logging.getLogger(__name__)


# --- config.py ---
load_dotenv()

redmine_token = os.getenv('REDMINE_TOKEN')
# interval = int(os.getenv('INTERVAL'))  # Sets interval between task running if needed

REDMINE_ISSUES_URL = 'https://redmine.twinscom.ru/issues.json'
REDMINE_TIME_ENTRIES_URL = 'https://redmine.twinscom.ru/time_entries.json'

# Настройки логгирования
log_level = logging.INFO
LOG_FILENAME = './src/logs/logfile.txt'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'

# Словарь user_id redmine: имя метки todoist
USER_IDS = {
    610: 'Катя',  # Todoist ID - 32780078
    539: 'Андрей',  # Todoist ID - 42263604
}

# --- end of config.py ---


def timetracking(foo):
    async def track_time():
        start_time = t.time()
        logger.info('--- Started working ---')

        await foo()

        end_time = t.time()
        total_time = round((end_time - start_time), 3)
        logger.info(f'--- Finished in {total_time} s. ---')

    return track_time


def set_preferences():

    # Задаем настройки модуля логгирования
    console_handler = logging.StreamHandler()
    logging.basicConfig(
                            force=True,
                            level=log_level,
                            format=LOG_FORMAT,
                            datefmt=LOG_DATEFMT,
                            handlers=[console_handler],
                        )

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)


class Redmine(ABC):
    """Класс для асинхронной работы с API Redmine"""

    _is_abstract = True

    def __init__(self):
        if self._is_abstract:
            raise NotImplementedError

        # Объявляется в подклассах
        self._url = None
        self._params = None

        self._headers = {
            'Content-Type': 'application/json',
            'X-Redmine-API-Key': redmine_token
        }

        self.raw_data = []
        self.data = None

    async def _create_single_task(self, session: ClientSession, params):
        """
        Формирует задачу для забора из Редмайна задач для одного пользователя.

        :param session: Объект aiohttp ссессии
        :param user_id: ID пользователя, для которого необходимо забрать задачи
        """

        request = session.get(self._url, headers=self._headers, params=params)
        return asyncio.create_task(request)

    @abstractmethod
    async def _download_data(self):
        pass

    async def get_data(self):
        """Забирает все задачи из Redmine и раскладывает их в словарь с ключами-id"""

        await self._download_data()
        return self.raw_data


class RedmineIssues(Redmine):
    _is_abstract = False

    def __init__(self):

        Redmine.__init__(self)
        self._url = REDMINE_ISSUES_URL

        self._params = {
            'tracker_id': '21',  # Внутренняя задача
            'created_on': '><2023-01-01|2023-03-31',
            'limit': 100,
            'status_id': '*',
            'subject': '~оценка',
        }

    async def _download_data(self):
        """Забирает все задачи для списка пользователей, который зафиксирован в config.py
        и складывает их в атрибут self.raw_data"""

        async with ClientSession() as session:

            gathered_tasks = []
            offset = 0

            for i in range(10):
                params_offset = {
                    'offset': offset
                }

                params = self._params | params_offset

                task = await self._create_single_task(session, params)
                gathered_tasks.append(task)
                offset += 100

            responses = await asyncio.gather(*gathered_tasks)

            self.raw_data = []
            for r in responses:
                temp = await r.json()
                self.raw_data.extend(temp['issues'])

        logger.info(f'Получено {len(self.raw_data)} задач из Redmine')

    def _change_keys(self):
        """Составляет словарь с ключом = ID Redmine"""

        self.data = {
            x.get('id'): x for x in self.raw_data
        }

    async def get_data(self):
        """Забирает все задачи из Redmine и раскладывает их в словарь с ключами-id"""

        self.raw_data = await Redmine.get_data(self)
        self._change_keys()

        return self.raw_data


class RedmineTime(Redmine):
    _is_abstract = False

    def __init__(self):
        Redmine.__init__(self)
        self._url = REDMINE_TIME_ENTRIES_URL
        self._params = {
            'limit': 100
        }
        self.issues = []

    def add_issues(self, issues: list):
        self.issues = issues

    async def _download_data(self):
        async with ClientSession() as session:
            gathered_tasks = []

            for t in self.issues:
                temp_params = {
                    'issue_id': t
                }
                params = self._params | temp_params

                task = await self._create_single_task(session, params)
                gathered_tasks.append(task)

            responses = await asyncio.gather(*gathered_tasks)

            for r in responses:
                temp = await r.json()
                temp = temp.get('time_entries')
                self.raw_data.extend(temp)


class RedmineTimeByProjects(RedmineTime):

    async def _download_data(self):
        async with ClientSession() as session:
            gathered_tasks = []

            for t in self.issues:
                temp_params = {
                    'project_id': t
                }
                params = self._params | temp_params

                task = await self._create_single_task(session, params)
                gathered_tasks.append(task)

            responses = await asyncio.gather(*gathered_tasks)

            for r in responses:
                temp = await r.json()
                temp = temp.get('time_entries')
                self.raw_data.extend(temp)


class RedmineTimeByUsers(RedmineTime):

    MAIN_ENTITY = 'time_entries'  # Ключевая сущность класса, которая будет загружена при возвращении ответа
    MAX_LIMIT = 100  # Максимальное количество записей в ответе API
    MAX_REQUESTS = 15  # Максимальное количество запросов в секунду
    raw_data = []

    def __init__(self, user, date_from):
        RedmineTime.__init__(self)

        self._params = self._params | {
            'user_id': user,
            'from': date_from,
        }

    @staticmethod
    async def _download_one_portion(gathered_tasks):
        """
        Отправляет несколько запросов и возвращает данные
        :param gathered_tasks: массив запросов, которые нужно отправить
        :return: список json-словарей с результатами
        """

        responses = await asyncio.gather(*gathered_tasks)

        res = []
        for r in responses:
            temp = await r.json()
            res.extend(temp.get(RedmineTimeByUsers.MAIN_ENTITY))

        return res

    async def _get_total_count(self, session):
        """
        Отправляет запрос на получение последней отбивки и извлекает из него значение общего количества записей
        :param session: текущая сессия, в которой идет работа
        :return: общее количество записей
        """

        params = self._params | {'limit': 1}
        async with session.get(self._url, headers=self._headers, params=params) as r:
            result = await r.json()
            return self._extract_total_count(result)

    @staticmethod
    def _extract_total_count(raw_data):
        """
        Извлекает общее количество записей из массива сырых полученных данных
        :param raw_data: сырые данные ответа
        :return: общее количество записей
        """
        return raw_data.get('total_count')

    async def _gather_tasks(self, session, total_rows_count):
        """
        Создает несколько задач и объединяет их для выполнения запроса.
        Явно задает в запросе параметр limit = MAX_LIMIT
        :param session: текущая сессия, в которой идет работа
        :param total_rows_count: общее количество записей, которые необходимо выгрузить
        :return: список задач, которые должны быть отправлены в одном цикле
        """

        gathered_portions = []
        gathered_tasks = []
        offset = 0

        while offset < total_rows_count:

            temp_params = {
                'limit': RedmineTimeByUsers.MAX_LIMIT,
                'offset': offset,
            }
            params = self._params | temp_params

            task = await self._create_single_task(session, params)
            gathered_tasks.append(task)

            if len(gathered_tasks) >= RedmineTimeByUsers.MAX_REQUESTS:
                gathered_portions.append(gathered_tasks)
                gathered_tasks = []

            offset += RedmineTimeByUsers.MAX_LIMIT

        if len(gathered_tasks) > 0:
            gathered_portions.append(gathered_tasks)

        return gathered_portions

    async def _download_data(self):
        """
        Основной метод, получает все запрошенные данные.
        Добавляет данные в переменную класса raw_data
        """

        async with ClientSession() as session:

            total_rows_count = await self._get_total_count(session)
            all_tasks = await self._gather_tasks(session, total_rows_count)

            for num, portion in enumerate(all_tasks):
                logger.info(f'Send request portion number {num+1}')
                temp = await self._download_one_portion(portion)
                RedmineTimeByUsers.raw_data.extend(temp)
                await asyncio.sleep(1)

    @classmethod
    def get(cls):
        return cls.raw_data


class RedmineIssuesByList(Redmine):
    _is_abstract = False
    MAX_LIMIT = 100
    MAX_REQUESTS = 15
    raw_data = []

    def __init__(self, issues_list: list):

        Redmine.__init__(self)
        self.issues_list = issues_list
        self._url = 'https://redmine.twinscom.ru/issues/'
        self._format = '.json'

    # Копия с Redmine._create_single_task(), только с другим url и без параметров
    async def _create_single_task_child(self, session: ClientSession, url):
        """
        Формирует задачу для забора из Редмайна задач для одного пользователя.

        :param session: Объект aiohttp ссессии
        :param user_id: ID пользователя, для которого необходимо забрать задачи
        """

        request = session.get(url, headers=self._headers)
        return asyncio.create_task(request)

    # Копия с RedmineTimeByUsers._gather_tasks()
    async def _gather_tasks(self, session):
        """
        Создает несколько задач и объединяет их для выполнения запроса.
        Явно задает в запросе параметр limit = MAX_LIMIT
        :param session: текущая сессия, в которой идет работа
        :param total_rows_count: общее количество записей, которые необходимо выгрузить
        :return: список задач, которые должны быть отправлены в одном цикле
        """

        gathered_portions = []
        gathered_tasks = []

        for issue in self.issues_list:

            url = self._url + str(issue) + self._format
            task = await self._create_single_task_child(session, url)
            gathered_tasks.append(task)

            if len(gathered_tasks) >= RedmineIssuesByList.MAX_REQUESTS:
                gathered_portions.append(gathered_tasks)
                gathered_tasks = []

        if len(gathered_tasks) > 0:
            gathered_portions.append(gathered_tasks)

        return gathered_portions

    # Копия с RedmineTimeByUsers._download_one_portion()
    @staticmethod
    async def _download_one_portion(gathered_tasks):
        """
        Отправляет несколько запросов и возвращает данные
        :param gathered_tasks: массив запросов, которые нужно отправить
        :return: список json-словарей с результатами
        """

        responses = await asyncio.gather(*gathered_tasks)

        res = []
        for r in responses:
            temp = await r.json()
            res.append(temp)

        return res

    async def _download_data(self):

        async with ClientSession() as session:
            all_tasks = await self._gather_tasks(session)

            for num, portion in enumerate(all_tasks):
                logger.info(f'Send request portion number {num+1}')
                temp = await self._download_one_portion(portion)
                RedmineIssuesByList.raw_data.extend(temp)
                await asyncio.sleep(1)

    @classmethod
    def get(cls):
        return cls.raw_data


class TransformToDf(ABC):

    def __init__(self):
        self.raw_data = None
        self.data = []

    def add(self, raw_data):
        self.raw_data = raw_data

    @abstractmethod
    def _clean(self):
        pass

    def transform(self):
        self._clean()
        self.data = pd.DataFrame(self.data)

        return self.data


class TransformIssuesToDf(TransformToDf):
    """Преобразует данные задач из Redmine в DataFrame"""

    def transform(self):
        self.data = TransformToDf.transform(self)
        self._get_delta()
        self._filter()
        self._group()

        self._reindex()
        return self.data

    def _clean(self):

        for row in self.raw_data:
            temp = {
                'id': row.get('id'),
                # 'assigned_to': row.get('assigned_to'),
                'author': row.get('author').get('name'),
                'created_on': row.get('created_on'),
                'updated_on': row.get('updated_on'),
                # 'start_date': row.get('start_date'),
                # 'closed_on': row.get('closed_on'),
                'estimated_hours': row.get('estimated_hours'),
                # 'status': row.get('status'),
                'subject': row.get('subject'),
                # 'tracker': row.get('tracker'),
                'project': row.get('project').get('name')
            }

            self.data.append(temp)

    def _get_delta(self):
        self.data[['created_on', 'updated_on']] = self.data[['created_on', 'updated_on']].apply(pd.to_datetime)
        self.data['delta'] = (self.data['updated_on'] - self.data['created_on'])
        self.data = self.data.drop(['created_on', 'updated_on'], axis=1)

    def _filter(self):
        sales_dep = [
            'Авдеева Яна',
            'Захарков Андрей',
            'Иванов Олег',
            'Милютин Юрий',
        ]

        self.data = self.data.loc[self.data['author'].isin(sales_dep)]

    def _group(self):
        self.data['type'] = self.data.apply(
            lambda row: self._group_by_type(row),
            axis=1
        )

    @staticmethod
    def _group_by_type(row):
        temp = row['subject'].lower()
        result = None

        if temp:
            match temp:
                case temp if 'seo' in temp:
                    result = 'SEO'
                case temp if 'авито' in temp:
                    result = 'Авито'
                case temp if 'реклам' in temp or 'директ' in temp or 'маркет' in temp or 'продвиж' in temp or 'телеграм' in temp:
                    result = 'Реклама'
                case temp if 'разраб' in temp or 'создан' in temp or 'доработ' in temp:
                    result = 'Разработка'
                case _:
                    result = 'Не удалось определить'

        return result

    def _reindex(self):
        self.data = self.data.reset_index().drop('index', axis=1)

    def get_issues_list(self):
        return self.data['id'].values.tolist()


class TransformTimeToDf(TransformToDf):
    """Преобразует данные расходов времени из Redmine в DataFrame"""

    def _clean(self):
        for row in self.raw_data:
            temp = {
                'hours': row.get('hours'),
                'issue': row.get('issue').get('id'),
                # 'user': row.get('user').get('name')
            }

            self.data.append(temp)

    def _group(self):
        self.data = self.data.groupby(['issue']).sum().reset_index()

    def transform(self):
        self.data = TransformToDf.transform(self)
        self._group()

        return self.data


class Factory:

    def __init__(self, factory_type):

        assert factory_type in ['issues', 'time']
        self.factory_type = factory_type

    def choose(self):

        match self.factory_type:
            case 'issues':
                return RedmineIssues(), TransformIssuesToDf()
            case 'time':
                return RedmineTime(), TransformTimeToDf()


class Extractor:

    def __init__(self, data):
        self.data = data
        self.clean_data = set()

    def extract_issues(self):

        for item in self.data:
            self.clean_data.add(item.get('issue').get('id'))

    def get(self):
        return list(self.clean_data)


class Cleaner:

    def __init__(self, data):
        self.data = data
        self.clean_data = []

    def clean(self):
        for raw_row in self.data:
            row = raw_row.get('issue')
            temp = {
                'id': row.get('id'),
                'project': row.get('project').get('name'),
                'tracker': row.get('tracker').get('name'),
                'author': row.get('author').get('name'),
                'subject': row.get('subject'),
                'description': row.get('description'),
                'start_date': row.get('start_date'),
                'due_date': row.get('due_date'),
                'estimated_hours': row.get('estimated_hours'),
                'spent_hours': row.get('spent_hours'),
            }
            self.clean_data.append(temp)

    def get(self):
        return self.clean_data


async def main():
    api, transformer = Factory('issues').choose()
    data = await api.get_data()

    transformer.add(data)
    issues_data = transformer.transform()

    issues = transformer.get_issues_list()

    api, transformer = Factory('time').choose()
    api.add_issues(issues)

    data = await api.get_data()
    transformer.add(data)
    time_data = transformer.transform()

    merged_data = issues_data.merge(time_data, how='outer', left_on='id', right_on='issue')
    merged_data = merged_data[['id', 'delta', 'subject', 'hours', 'type', 'author', 'project']]

    print(merged_data)


@timetracking
async def main2():

    logging.info('Work with API')
    for user in list(USER_IDS.keys()):
        api = RedmineTimeByUsers(user, date_from='2022-01-01')
        await api.get_data()

    data = RedmineTimeByUsers.get()

    extractor = Extractor(data)
    extractor.extract_issues()
    issues = extractor.get()

    logger.info(f'Got {len(issues)} issues, start getting from Redmine')

    issue_extractor = RedmineIssuesByList(issues)
    await issue_extractor.get_data()
    issue_data = issue_extractor.get()

    cleaner = Cleaner(issue_data)
    cleaner.clean()
    issue_data = cleaner.get()

    df = pd.DataFrame(issue_data)
    df.to_excel('issues.xlsx')


    # logging.info('Work with data')
    # clean_data = []
    # for row in data:
    #     temp = {

    #     }
    #
    #     clean_data.append(temp)
    #
    # df = pd.DataFrame(clean_data)
    # print(df)


if __name__ == '__main__':
    set_preferences()
    asyncio.run(main2())
