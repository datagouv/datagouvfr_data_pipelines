from tenacity import retry, stop_after_attempt, wait_exponential
import requests


def log_retry_attempt(state):
    exception = state.outcome.exception()
    exception_name = type(exception).__name__
    exception_message = str(exception)
    print(f"Retrying {state.fn.__name__} due to {exception_name}: {exception_message}")


def _simple_connection_retry(
    attempts=5,
    func=log_retry_attempt,
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
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


@simple_connection_retry
def get_with_retries(url: str, **kwargs):
    return requests.get(url, **kwargs)
