import os
import asyncio

import pytest

from connect.metrika import Metrika


@pytest.mark.asyncio
async def test_response():

    token = os.getenv('METRIKA_TOKEN')
    counter = os.getenv('METRIKA_COUNTER')

    connection = Metrika()
    data = await connection.get(token, counter)
    print(data)

    assert data
