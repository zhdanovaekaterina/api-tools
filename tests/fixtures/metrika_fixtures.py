import os
import pytest

from dotenv import load_dotenv

import connect.metrika
from connect.metrika import Metrika
from tests.mocks import metrika_mocks


@pytest.fixture()
def valid_metrika_connection(monkeypatch):
    """Returns valid Metrika connection for developing."""

    monkeypatch.setattr(connect.metrika.Metrika, "_get", metrika_mocks.data_get)
    return Metrika(token='', counter='')


@pytest.fixture()
def invalid_metrika_connection(monkeypatch):
    """Returns invalid Metrika connection with 403 error."""

    monkeypatch.setattr(connect.metrika.Metrika, "_get", metrika_mocks.mock_403_get)
    return Metrika(token='', counter='')


@pytest.fixture()
def real_metrika_connection(monkeypatch):
    """Returns valid Metrika connection for developing."""

    load_dotenv()
    token = os.getenv('METRIKA_TOKEN')
    counter = os.getenv('METRIKA_COUNTER')

    return Metrika(token=token, counter=counter)
