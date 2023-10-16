from . import _comma_separated_to_tuple


def test_no_commas():
    assert _comma_separated_to_tuple("hello") == ("hello",)


def test_one_comma():
    assert _comma_separated_to_tuple("hello,world") == ("hello", "world")


def test_empty_cell():
    assert _comma_separated_to_tuple("hello,,world") == ("hello", "", "world")


def test_only_empty_cells():
    assert _comma_separated_to_tuple(",,,,") == ("", "", "", "", "")


def test_double_quoted_value():
    assert _comma_separated_to_tuple('"hello,world"') == ("hello,world",)


def test_apostrophe():
    assert _comma_separated_to_tuple("what's up,nothin' much") == (
        "what's up",
        "nothin' much",
    )


def test_double_quoted_value_with_comma():
    assert _comma_separated_to_tuple('"hello,world",goodbye') == (
        "hello,world",
        "goodbye",
    )
