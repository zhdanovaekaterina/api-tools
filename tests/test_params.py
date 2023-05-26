import pytest

import connect.base.params as params


testdata = [
    ('Metrika', params.MetrikaParams),
    ('metrika', params.MetrikaParams),
]


@pytest.mark.parametrize('label,expected', testdata)
def test_params_construction(label, expected):
    """
    This tests Params object initiation.
    :param label:
    :param expected:
    :return:
    """

    obj = params.Params(label).create()
    assert type(obj) == expected


def test_invalid_params():
    """
    This tests if correct error will be raised when user tries to get unpredictable Params object.
    :return:
    """

    correct_error_class = AttributeError
    invalid_label = 'something_strange'

    with pytest.raises(correct_error_class) as exc_info:
        obj = params.Params(invalid_label).create()

    exception_class = exc_info.__getattribute__('_excinfo')[0]
    assert exception_class == correct_error_class
