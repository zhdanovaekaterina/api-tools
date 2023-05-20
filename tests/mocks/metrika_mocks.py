async def mock_403_get(self, *args):
    return {
        'errors':
            [{'error_type': 'invalid_token', 'message': 'Invalid oauth_token'}],
        'code': 403,
        'message': 'Invalid oauth_token'
    }
