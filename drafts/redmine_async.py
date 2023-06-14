import os
import asyncio
import logging
from abc import ABC, abstractmethod

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


if __name__ == '__main__':
    set_preferences()
    asyncio.run(main())

    # with aiomisc.entrypoint(log_config=False) as loop:
    #     set_preferences()

        # # If need to run while True
        # try:
        #     while True:
        #         task = loop.create_task(main())
        #         loop.run_until_complete(task)
        #         sleep(interval)
        # except KeyboardInterrupt:
        #     logger.info('Программа остановлена вручную.')
