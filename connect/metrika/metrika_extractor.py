import logging

from aiohttp import ClientSession

logger = logging.getLogger(__name__)


class MetrikaApiError:
    """
    A simple class to represent API errors
    """

    LIST_OF_DIMENTIONS_AND_METRICS = 'https://yandex.com/dev/metrika/doc/api2/api_v1/attrandmetr/dim_all.html'

    def __init__(self, **kwargs):
        self.code = kwargs.get('code', 'Unexpected')
        self.message = kwargs.get('message', 'An API Error occured')
        self._change_message()

    def _change_message(self):
        """
        Adds more valuable comment to some API errors.
        """

        match self.code:
            case 400:
                self.message = f'{self.message}. ' \
                               f'Documentation link: {MetrikaApiError.LIST_OF_DIMENTIONS_AND_METRICS}'

    def __str__(self):
        return f'API Error: {self.code}: {self.message}'

    def __repr__(self):
        return f'API Error: {self.code}: {self.message}'


class MetrikaConsts:
    """
    Contains constant values for Metrika API, like urls
    """

    BASE_URL = 'https://api-metrika.yandex.net/stat/'
    VERSION = 'v1'


class MetrikaExtractor:
    """
    Implements interface for Yandex Metrika API.
    Docs: https://yandex.ru/dev/metrika/doc/api2/concept/about.html
    """

    def __init__(self, token, counter):
        logging.info('Metrika init')

        self.raw_response = None
        self.response = None

        self.consts = MetrikaConsts
        self.counter = counter

        self.headers = {
            "Authorization": "OAuth " + token,
        }

        self.params = {
            'ids': self.counter,
        }

    def add_params(self, params: dict):
        """
        Adds params to the object.
        """

        self.params = self.params | params

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

        endpoint = self.consts.BASE_URL + self.consts.VERSION + '/data'

        async with ClientSession() as session:
            self.raw_response = await self._get(session, endpoint, self.headers, self.params)

        if self.raw_response.get('errors'):
            return MetrikaApiError(code=self.raw_response.get("code"), message=self.raw_response.get("message"))

        return self.raw_response
