import asyncio

import pytest
from dotenv import load_dotenv


@pytest.fixture(scope='session', autouse=True)
def loop():
    load_dotenv()
    yield

    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    #
    # yield loop
    #
    # loop.close()
