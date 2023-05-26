async def mock_403_get(self, *args):
    return {
        'errors':
            [{'error_type': 'invalid_token', 'message': 'Invalid oauth_token'}],
        'code': 403,
        'message': 'Invalid oauth_token'
    }


async def mock_400_get(self, *args):
    return {
        'code': 400,
        'errors': [
            {
                'error_type': 'invalid_parameter',
                'message': 'Metrics and dimensions with different prefixes can be '
                    'used together only when filtering data, value: '
                    'ym:pv:date is incompatible with ym:s:users, error '
                    'code: 4011'
            }
        ],
            'message': 'Metrics and dimensions with different prefixes can be used '
                'together only when filtering data, value: ym:pv:date is '
                'incompatible with ym:s:users, error code: 4011'
    }


async def data_get(self, *args):
    return {
        'data': [{'dimensions': [{'name': '2023-05-17'}], 'metrics': [1524.0]},
                 {'dimensions': [{'name': '2023-05-18'}], 'metrics': [1200.0]},
                 {'dimensions': [{'name': '2023-05-22'}], 'metrics': [403.0]},
                 {'dimensions': [{'name': '2023-05-23'}], 'metrics': [285.0]},
                 {'dimensions': [{'name': '2023-05-19'}], 'metrics': [272.0]},
                 {'dimensions': [{'name': '2023-05-20'}], 'metrics': [268.0]},
                 {'dimensions': [{'name': '2023-05-21'}], 'metrics': [256.0]}],
        'data_lag': 0,
        'max': [1524.0],
        'min': [256.0],
        'query': {'adfox_event_id': '0',
                  'attr_name': '',
                  'attribution': 'LastSign',
                  'auto_group_size': '1',
                  'currency': 'RUB',
                  'date1': '2023-05-17',
                  'date2': '2023-05-23',
                  'dimensions': ['ym:s:date'],
                  'group': 'Week',
                  'ids': [1276975],
                  'limit': 100,
                  'metrics': ['ym:s:users'],
                  'offline_window': '21',
                  'offset': 1,
                  'quantile': '50',
                  'sort': ['-ym:s:users']},
        'sample_share': 1.0,
        'sample_size': 4405,
        'sample_space': 4405,
        'sampled': False,
        'total_rows': 7,
        'total_rows_rounded': False,
        'totals': [4063.0]
    }

