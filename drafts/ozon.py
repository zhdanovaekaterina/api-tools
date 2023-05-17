import json
from pprint import pprint

import requests


class OzonConfig:

    token = os.getenv('OZON_TOKEN')
    client_id = os.getenv('OZON_CLIENT_ID')

    HOST = 'https://api-seller.ozon.ru/'
    VERSION = 'v1'


endpoint = '/analytics/data'
# full_url = OzonConfig.HOST + OzonConfig.VERSION + endpoint
full_url = 'https://api-seller.ozon.ru/v1/analytics/data'

headers = {
    'Host': 'api-seller.ozon.ru',
    'Client-Id': OzonConfig.client_id,
    'Api-Key': OzonConfig.token,
    'Content-Type': 'application/json',
}

params = {
    'date_from': '2023-04-03',
    'date_to': '2023-04-03',
    'dimension': [
        'sku',
        'day'
    ],
    'metrics': [
        'revenue',
        'ordered_units',
        'delivered_units',
        'adv_sum_all'
    ],
    'limit': 1000,
    'offset': 0,
}

data = json.dumps(params)

result_raw = requests.post(full_url, headers=headers, data=data)
result = json.loads(result_raw.text)

pprint(result)
