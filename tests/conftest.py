import pytest
from dotenv import load_dotenv


@pytest.fixture(scope='session', autouse=True)
def set_up():
    load_dotenv()
    yield
