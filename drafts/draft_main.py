from abc import ABC, abstractmethod


class Params(dict):

    def set_period(self, period='last_month'):
        pass


class Connection(ABC):
    """
    This class implements basic API methods and interface
    """

    def __init__(self, **kwargs):
        self.token = kwargs.get('token')
        self.params = kwargs.get('params')

    def _get(self):
        """
        Will implement async GET request
        :return:
        """
        pass

    def _post(self):
        """
        Will implement async POST request
        :return:
        """
        pass

    @abstractmethod
    def get(self):
        """
        Will implement api connection
        :return:
        """
        pass


class MetrikaConnection(Connection):

    def __init__(self, **kwargs):
        Connection.__init__(self, kwargs)

    def get(self):
        pass


class Report(ABC):

    def __init__(self):
        self.data = None

    def add(self, data):
        """
        Adds data to the object
        :return:
        """
        self.data = data

    def get(self):
        """
        Returns data
        :return:
        """
        return self.data

    @abstractmethod
    def load(self):
        """
        Loads data to the destination
        :return:
        """
        pass


class RawReport(Report):
    pass


class DataFrameReport(Report):
    """
    Will be the report by default
    """

    pass


class ExcelReport(Report):

    def __init__(self, file_name):
        self.file_name = file_name


class Tool:

    def __init__(self, source: Connection, to: Report = DataFrameReport()):
        self.source = source
        self.to = to

    def get(self):
        """
        Will implement api connection and report composing
        :return:
        """

        raw_data = self.source.get()
        self.to.add(raw_data)
        self.to.load()

        data = self.to.get()

        if data:
            return data


# <-- Settings constants section -->

TOKEN = 'secrettoken'
PERIOD = 'last month'   # Here can also be one or two dates;
                        # one date will mean period from this date to yesterday,
                        # two dates - concrete period
REPORT = 'conversions report'

# <-- Main interface section -->

params = Params()
params.set_period()  # This will set period by default or by label passed
params['custom_param'] = 'custom_param'

connection = MetrikaConnection(
    token=TOKEN,
    params=params
)

load_to = ExcelReport(
    file_name='file_name.xlsx'
)

tool = Tool(
    source=connection,
    to=load_to,  # Here should be an opportunity not to pass а param and get DataFrame instead
)

data = tool.get()
print(data)
