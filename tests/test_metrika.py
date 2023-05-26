from pprint import pprint

import pytest

import connect.metrika
from connect.metrika import ApiError
from connect.metrika import Metrika
from tests.mocks import metrika_mocks


@pytest.mark.asyncio
async def test_valid_params(monkeypatch):
    """This will test valid data getting"""

    monkeypatch.setattr(connect.metrika.Metrika, "_get", metrika_mocks.data_get)
    connection = Metrika(token='', counter='')

    correct_params = {
        'metrics': 'ym:s:users',
        'dimensions': 'ym:s:date',
    }

    connection.add_params(correct_params)
    data = await connection.get()
    assert type(data) == dict


@pytest.mark.asyncio
async def test_invalid_params(monkeypatch):
    """This will test valid data getting"""

    monkeypatch.setattr(connect.metrika.Metrika, "_get", metrika_mocks.mock_400_get)
    connection = Metrika(token='', counter='')

    incorrect_params = {
        'metrics': 'ym:s:users',
        'dimensions': 'ym:pv:date',
    }

    connection.add_params(incorrect_params)
    data = await connection.get()

    assert type(data) == ApiError
    assert data.code == 400


@pytest.mark.asyncio
async def test_403_response(monkeypatch):
    """This will test 403 response proceeding"""

    monkeypatch.setattr(connect.metrika.Metrika, "_get", metrika_mocks.mock_403_get)
    connection = Metrika(token='', counter='')

    data = await connection.get()
    assert type(data) == ApiError
    assert data.code == 403
