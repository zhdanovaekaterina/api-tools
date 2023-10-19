"""
Словарь с допустимыми методами
"""


from enum import Enum


class Method(Enum):
    """
    Словарь допустимых методов API-вызовов
    """

    GET = 'GET'
    POST = 'POST'
