"""Type-safe accessors for BeautifulSoup elements."""

from bs4 import Tag


def require_tag(tag: Tag | None, description: str) -> Tag:
    """Return the tag or raise if the expected element is missing."""
    if tag is None:
        raise ValueError(f"missing {description}")
    return tag


def require_str_attr(tag: Tag, name: str) -> str:
    """Return attribute `name` as a string or raise if it is missing or not a string."""
    value = tag.get(name)
    if not isinstance(value, str):
        raise ValueError(f"missing {name!r} attribute on <{tag.name}>")
    return value
