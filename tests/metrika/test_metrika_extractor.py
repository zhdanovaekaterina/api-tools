import pytest

from connect.metrika import metrika_extractor
from connect.metrika.metrika_extractor import MetrikaApiError, MetrikaExtractor
from tests.mocks import metrika_mocks


@pytest.mark.asyncio
async def test_valid_params(monkeypatch):
    """This will test valid data getting"""

    monkeypatch.setattr(metrika_extractor.MetrikaExtractor, "_get", metrika_mocks.data_get)
    connection = MetrikaExtractor(token='', counter='')

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

    monkeypatch.setattr(metrika_extractor.MetrikaExtractor, "_get", metrika_mocks.mock_400_get)
    connection = MetrikaExtractor(token='', counter='')

    incorrect_params = {
        'metrics': 'ym:s:users',
        'dimensions': 'ym:pv:date',
    }

    connection.add_params(incorrect_params)
    data = await connection.get()

    assert type(data) == MetrikaApiError
    assert data.code == 400


@pytest.mark.asyncio
async def test_403_response(monkeypatch):
    """This will test 403 response proceeding"""

    monkeypatch.setattr(metrika_extractor.MetrikaExtractor, "_get", metrika_mocks.mock_403_get)
    connection = MetrikaExtractor(token='', counter='')

    data = await connection.get()
    assert type(data) == MetrikaApiError
    assert data.code == 403
