import logging

from aiohttp import ClientSession


logger = logging.getLogger(__name__)


class ApiError:
    """
    A simple class to represent API errors
    """

    def __init__(self, **kwargs):
        self.code = kwargs.get('code', 'Unexpected')
        self.message = kwargs.get('message', 'An API Error occured')

    def __str__(self):
        return f'API Error: {self.code}: {self.message}'

    def __repr__(self):
        return f'API Error: {self.code}: {self.message}'


class MetrikaConsts:
    """
    Contains constant values for Metrika API, like urls
    """

    main_url = 'https://api-metrika.yandex.net/stat/'
    version = 'v1'


class Metrika:
    """
    Implements interface for Yandex Metrika API.
    Docs: https://yandex.ru/dev/metrika/doc/api2/concept/about.html
    """

    def __init__(self, token, counter):
        logging.info('Metrika init')

        self.consts = MetrikaConsts
        self.counter = counter

        self.headers = {
            "Authorization": "OAuth " + token,
        }

        self.params = {
            'ids': self.counter,
            # 'metrics': 'ym:s:users',
            # 'dimensions': 'ym:s:date',
        }

    async def _get(self, session, url, headers, params):
        """
        Sends request to the API.
        :param session:
        :param url:
        :param headers:
        :param params:
        :return:
        """

        async with session.get(url, headers=headers, params=params) as resp:
            return await resp.json()

    async def get(self):
        """
        Connect to the endpoint and returns data.
        :return:
        """

        endpoint = self.consts.main_url + self.consts.version + '/data'

        params = self.params

        async with ClientSession() as session:
            response = await self._get(session, endpoint, self.headers, params)

        if response.get('errors'):
            return ApiError(code=response.get("code"), message=response.get("message"))

        return response
