from typing import Callable

from tenacity import retry, stop_after_attempt, wait_exponential
import requests


def log_retry_attempt(state):
    exception = state.outcome.exception()
    exception_name = type(exception).__name__
    exception_message = str(exception)
    print(f"Retrying {state.fn.__name__} due to {exception_name}: {exception_message}")


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
    pass


# re-implement requests basic methods with a retry
for _method in ["get", "post", "put", "patch", "head", "delete", "options"]:

    @simple_connection_retry
    def _m(cls, url: str, _method=_method, **kwargs):
        return getattr(requests, _method)(url, **kwargs)

    setattr(RequestRetry, _method, classmethod(_m))
