from abc import ABC, abstractmethod


class BaseConnectorInterface(ABC):
    """Gives basic interface for API connection"""

    @abstractmethod
    def get(self, **kwargs):
        """
        Connects to the API endpoint and returns raw result.

        :return:
        """
        pass


class BaseConnector:
    """Implements basic interface for API connection"""

    def __init__(self, **kwargs):
        """
        Configures class with credentials passed.

        :param kwargs:
        """
        pass
