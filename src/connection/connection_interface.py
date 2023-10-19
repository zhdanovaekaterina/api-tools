"""
Интерфейс для классов-подключений
"""


from abc import ABC, abstractmethod

from ..dict.method import Method
from ..dict.target import Target


class ConnectionInterface(ABC):
    """
    Интерфейс для классов-подключений
    """

    @abstractmethod
    def method(self, method: Method):
        """
        Задает метод для подключения
        :param method
        :return: self
        """

    @abstractmethod
    def endpoint(self, endpoint: str):
        """
        Задает точку для подключения
        :param endpoint
        :return: self
        """

    @abstractmethod
    def headers(self, headers: dict):
        """
        Задает пользовательские headers
        :param headers
        :return: self
        """

    @abstractmethod
    def params(self, params: dict):
        """
        Задает параметры запроса
        :param params
        :return: self
        """

    @abstractmethod
    def target(self, target: Target):
        """
        Задает параметры запроса
        :param target
        :return: self
        """

    @abstractmethod
    def get(self):
        """
        Выполняет запрос
        :param target
        :return: self
        """
