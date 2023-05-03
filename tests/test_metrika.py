import os


def test_metrika():

    from tools.metrika import Metrika

    test_env = os.getenv('TOKEN')
    print(test_env)

    metrika_obj = Metrika()

    # TODO: find out why this doesn't work from console
