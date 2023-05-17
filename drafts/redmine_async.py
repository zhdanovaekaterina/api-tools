import asyncio
import json
import logging
import time as t
from logging.handlers import TimedRotatingFileHandler
from aiohttp import ClientSession

from src import config as c

logger = logging.getLogger(__name__)


class Redmine:
    """Класс для асинхронной работы с API Redmine"""

    def __init__(self):
        self._url = c.REDMINE_URL

        self._headers = {
            'Content-Type': 'application/json',
            'X-Redmine-API-Key': c.redmine_token
        }

        self.raw_data = None
        self.data = None

    async def single_user_tasks(self, session: ClientSession, user_id: int):
        """
        Формирует задачу для забора из Редмайна задач для одного пользователя.

        :param session: Объект aiohttp ссессии
        :param user_id: ID пользователя, для которого необходимо забрать задачи
        """

        params = {
            'assigned_to_id': user_id
        }

        request = session.get(self._url, headers=self._headers, params=params)
        return asyncio.create_task(request)

    async def _download_data(self):
        """Забирает все задачи для списка пользователей, который зафиксирован в config.py
        и складывает их в атрибут self.raw_data"""

        async with ClientSession() as session:

            gathered_tasks = []
            for user in list(c.user_ids.keys()):
                task = await self.single_user_tasks(session, user)
                gathered_tasks.append(task)

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

    @property
    async def get_data(self):
        """Забирает все задачи из Redmine и раскладывает их в словарь с ключами-id"""

        await self._download_data()
        self._change_keys()

        return self.data
