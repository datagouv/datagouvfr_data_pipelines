import pytest
from bs4 import BeautifulSoup
from datagouvfr_data_pipelines.utils.bs import require_str_attr, require_tag


def _find(html: str, name: str):
    tag = BeautifulSoup(html, "html.parser").find(name)
    assert tag is not None
    return tag


def test_require_tag_returns_the_element():
    tag = _find('<meta property="og:description" content="hello">', "meta")
    assert require_tag(tag, "meta") is tag


def test_require_tag_raises_when_missing():
    with pytest.raises(ValueError, match="missing og:description meta tag"):
        require_tag(None, "og:description meta tag")


def test_require_str_attr_returns_the_string():
    tag = _find('<meta property="og:description" content="hello">', "meta")
    assert require_str_attr(tag, "content") == "hello"


def test_require_str_attr_raises_when_missing():
    tag = _find("<meta>", "meta")
    with pytest.raises(ValueError, match="missing 'content' attribute on <meta>"):
        require_str_attr(tag, "content")


def test_require_str_attr_raises_on_a_list():
    tag = _find('<span class="a b"></span>', "span")
    with pytest.raises(ValueError, match="missing 'class' attribute on <span>"):
        require_str_attr(tag, "class")
