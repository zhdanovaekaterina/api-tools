"""
Базовый интерфейс работы классов-подключений
"""


from ..connection.connection_interface import ConnectionInterface
from ..dict.method import Method
from ..dict.target import Target
from ..config.message import Message


class BaseConnection(ConnectionInterface):
    """
    Базовый абстрактный класс для реализации основных методов
    """

    def __init__(self, **kwargs):

        # Инициализация переменных класса
        self.method = None
        self.endpoint = None
        self.headers = None
        self.params = None
        self.target = None

    def _get(self):
        """
        Отправляет GET-запрос
        :return:
        """
        pass

    def _post(self):
        """
        Отправляет POST-запрос
        :return:
        """
        pass

    def method(self, method: Method):
        """
        Задает метод для подключения
        :param method
        :return: self
        """

        if not isinstance(method, Method):
            raise TypeError(Message.wrong_type(method, Method))

        self.method = method
        return self

    def endpoint(self, endpoint: str):
        """
        Задает точку для подключения
        :param endpoint
        :return: self
        """
        self.endpoint = endpoint
        return self

    def headers(self, headers: dict):
        """
        Задает пользовательские headers
        :param headers
        :return: self
        """
        self.headers = headers
        return self

    def params(self, params: dict):
        """
        Задает параметры запроса
        :param params
        :return: self
        """
        self.params = params
        return self

    def target(self, target: Target):
        """
        Задает параметры запроса
        :param target
        :return: self
        """

        if not isinstance(target, Target):
            raise TypeError(Message.wrong_type(target, Target))

        self.target = target
        return self

    def get(self):
        """
        Выполняет запрос и возвращает результат
        :return:
        """
        pass
