import asyncio
import logging

from aiohttp import ClientSession


logger = logging.getLogger(__name__)


class Metrika:
    """
    Implements interface for Yandex Metrika API.
    Docs: https://yandex.ru/dev/metrika/doc/api2/concept/about.html
    """

    def __init__(self):
        logging.info('Metrika init')

    async def get(self, token, counter):
        """
        Connect to the endpoint and returns data.
        :return:
        """

        url = 'https://api-metrika.yandex.net/stat/v1/data'

        headers = {
            "Authorization": "OAuth " + token,
        }

        params = {
            'ids': counter,
            'metrics': 'ym:s:users',
        }

        async with ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                response = await resp.read()

        return response
