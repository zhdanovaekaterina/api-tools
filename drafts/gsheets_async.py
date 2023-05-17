import json
import logging

from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds

from src import config as c

logger = logging.getLogger(__name__)


class GSheets:
    """Класс для асинхронной работы с API Google Sheets"""

    def __init__(self):
        """Инициализирует доступы"""

        service_account_key = json.loads(c.google_config, strict=False)

        self.creds = ServiceAccountCreds(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
            ],
            **service_account_key
        )

        self.account = Aiogoogle(service_account_creds=self.creds)

    @staticmethod
    async def _connect(acc):
        """Создает и возвращает подключение к GSheets"""
        return await acc.discover('sheets', 'v4')

    async def get_data(self):
        """Подключается к GSheets и получает данные указанного диапазона"""

        async with self.account as acc:
            conn = await self._connect(acc)
            request = conn.spreadsheets.values.get(spreadsheetId=c.google_sheet, range=c.google_range)
            result = await acc.as_service_account(request)

            logger.info('Получен список задач из базы')
            return result.get('values')
