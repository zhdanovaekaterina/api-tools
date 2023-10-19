import pytest

from src.connection.base_connection import BaseConnection


@pytest.mark.asyncio
async def test_create():
    """
    Тест создания экземпляра класса-соединения
    :return:
    """

    object = BaseConnection()
