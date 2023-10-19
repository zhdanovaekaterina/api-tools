# <-- Settings constants section -->

TOKEN = 'secrettoken'


# <-- Main interface section -->

method = 'GET'  # Enum
endpoint = '/visits/list'
params = {
    'period': 'last week',
    'custom_param': 'custom_param'
}
load_to = 'excel'  # Enum

report = MetrikaConnection(token=TOKEN) \
    .method(method)\
    .endpoint(endpoint) \
    .headers()  # Optional, will replace current headers if needed
    .params(params)\
    .target(load_to)\  # Optional, if not defined will return DataFrame object
    .asJson()  # Optional, will change response format
    .get()  # Will send request


# All inner constants like base url of the api should be in lib config
