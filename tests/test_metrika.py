from pprint import pprint

import pytest

from connect.metrika import ApiError
from tests.fixtures.metrika_fixtures import real_metrika_connection, invalid_metrika_connection


@pytest.mark.asyncio
async def test_response(real_metrika_connection):
    data = await real_metrika_connection.get()
    pprint(data)


@pytest.mark.asyncio
async def test_403_response(invalid_metrika_connection):

    data = await invalid_metrika_connection.get()

    assert type(data) == ApiError
    assert data.code == 403
