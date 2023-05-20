import os
import pytest

import connect.metrika
from connect.metrika import Metrika
from tests.mocks import metrika_mocks


@pytest.fixture()
def real_metrika_connection():
    """Returns real Metrika connection for developing."""
    token = os.getenv('METRIKA_TOKEN')
    counter = os.getenv('METRIKA_COUNTER')
    connection = Metrika(token=token, counter=counter)

    return connection


@pytest.fixture()
def invalid_metrika_connection(monkeypatch):
    """Returns invalid Metrika connection with 403 error."""

    monkeypatch.setattr(connect.metrika.Metrika, "_get", metrika_mocks.mock_403_get)
    return Metrika(token='', counter='')
