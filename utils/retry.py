import logging
from typing import Callable

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


def log_retry_attempt(state):
    exception = state.outcome.exception()
    exception_name = type(exception).__name__
    exception_message = str(exception)
    logging.warning(
        f"Retrying {state.fn.__name__} due to {exception_name}: {exception_message}"
    )


def _simple_connection_retry(
    attempts: int = 5,
    func: Callable = log_retry_attempt,
    wait=wait_exponential(multiplier=10, max=60),
    reraise: bool = True,
    **kwargs,
):
    return retry(
        stop=stop_after_attempt(attempts),  # Stop after 5 attempts
        wait=wait,  # Exponential backoff
        before_sleep=func,  # Call this function before each retry
        reraise=reraise,  # Reraise the exception after all retries are exhausted,
        **kwargs,
    )


simple_connection_retry = _simple_connection_retry()


class RequestRetry:
    """requests methods re-implemented with a retry."""

    @classmethod
    @simple_connection_retry
    def get(cls, url: str, **kwargs) -> requests.Response:
        return requests.get(url, **kwargs)

    @classmethod
    @simple_connection_retry
    def post(cls, url: str, **kwargs) -> requests.Response:
        return requests.post(url, **kwargs)

    @classmethod
    @simple_connection_retry
    def put(cls, url: str, **kwargs) -> requests.Response:
        return requests.put(url, **kwargs)

    @classmethod
    @simple_connection_retry
    def patch(cls, url: str, **kwargs) -> requests.Response:
        return requests.patch(url, **kwargs)

    @classmethod
    @simple_connection_retry
    def head(cls, url: str, **kwargs) -> requests.Response:
        return requests.head(url, **kwargs)

    @classmethod
    @simple_connection_retry
    def delete(cls, url: str, **kwargs) -> requests.Response:
        return requests.delete(url, **kwargs)

    @classmethod
    @simple_connection_retry
    def options(cls, url: str, **kwargs) -> requests.Response:
        return requests.options(url, **kwargs)
