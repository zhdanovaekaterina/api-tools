class Params:
    """
    This is a public interface for params construction.
    """

    def __init__(self, system: str):
        """
        The constructor will make an object based on system param.
        :param system: this shows, for which system an object will be configured.
        """

        self.system = system

    def create(self):
        return self._choose()

    def _choose(self):
        """
        A factory method to choose needed system among one of the children.
        :param system:
        :return:
        """

        match self.system.lower():
            case 'metrika':
                return MetrikaParams()
            case _:
                raise AttributeError('Please pass a valid param!')


class BaseParams(dict):
    """
    This class contains params for API requests.
    """

    def __init__(self):
        pass


class MetrikaParams(BaseParams):
    """
    This class contains params for Metrika API request.
    """

    def __init__(self):
        pass
