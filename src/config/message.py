"""
Технические сообщения
"""


from dataclasses import dataclass


@dataclass
class Message:
    """
    Технические сообщения
    """

    @staticmethod
    def wrong_type(obj, cls):
        """
        Сообщение о неправильном типе переданного аргумента
        :param obj:
        :param cls:
        :return:
        """

        return f'Passed param {obj} should be an instance of {cls}'
