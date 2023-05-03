import os
import sys

import pytest
from dotenv import load_dotenv


@pytest.fixture(scope='session', autouse=True)
def set_up_session():
    load_dotenv()

    # Adding '/tools' directory to Python path
    script_dir = os.path.dirname(__file__)
    tools_dir = os.path.abspath(os.path.join(script_dir, os.pardir, 'tools'))
    sys.path.append(tools_dir)

    yield
