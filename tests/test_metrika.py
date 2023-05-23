from pprint import pprint

import pytest

from connect.metrika import ApiError
from tests.fixtures.metrika_fixtures import real_metrika_connection, valid_metrika_connection, invalid_metrika_connection


# TODO: return valid_metrika_connection fixture after exploring
@pytest.mark.asyncio
async def test_response(real_metrika_connection):
    """This will test valid data getting"""

    data = await real_metrika_connection.get()
    pprint(data)

    # assert len(data.get('data')) == 7


@pytest.mark.asyncio
async def test_403_response(invalid_metrika_connection):
    """This will test 403 response proceeding"""

    data = await invalid_metrika_connection.get()

    assert type(data) == ApiError
    assert data.code == 403
