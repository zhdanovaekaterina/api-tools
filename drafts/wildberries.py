import os

from dotenv import load_dotenv
load_dotenv()

import json
from pprint import pprint

import requests


class WildberriesConfig:

    token = os.getenv('WILDBERRIES_TOKEN')

    HOST = 'https://statistics-api.wildberries.ru/api/'
    VERSION = 'v1'


endpoint = '/supplier/sales'
full_url = WildberriesConfig.HOST + WildberriesConfig.VERSION + endpoint

headers = {
    'Authorization': WildberriesConfig.token,
}

params = {
    'dateFrom': '2023-04-01',
}

# data = json.dumps(params)

result_raw = requests.get(full_url, headers=headers, params=params)
print(result_raw.status_code)
pprint(result_raw.text)
