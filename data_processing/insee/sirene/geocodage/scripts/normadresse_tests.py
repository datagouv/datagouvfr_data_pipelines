import yaml

from normadresse import abrev


# Magic: Generate tests from yml.
def pytest_generate_tests(metafunc):
    if {'input', 'expected'} <= set(metafunc.fixturenames):
        with open("tests.yml") as fixtures:
            metafunc.parametrize("input,expected", yaml.load(fixtures).items())


def test_abrev(input, expected):
    assert abrev(input) == expected
