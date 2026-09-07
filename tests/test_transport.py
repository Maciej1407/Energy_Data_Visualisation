"""The single retry loop."""

import pytest

from energyviz.config import RetryPolicy, Settings
from energyviz.errors import FetchError
from energyviz.transport import HttpClient


class Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class Session:
    """A session that replays a scripted sequence of outcomes."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def client_for(outcomes, attempts=3):
    slept = []
    settings = Settings(retry=RetryPolicy(attempts=attempts, delay_seconds=7.0))
    return HttpClient(settings, session=Session(outcomes), sleep=slept.append), slept


def test_a_first_attempt_success_does_not_sleep():
    client, slept = client_for([Response(200, {"data": []})])

    assert client.get_json("http://x", {}) == {"data": []}
    assert slept == []


def test_it_retries_past_a_transport_error_and_a_bad_status():
    client, slept = client_for([
        OSError("connection reset"),
        Response(503),
        Response(200, {"data": [1]}),
    ])

    assert client.get_json("http://x", {}) == {"data": [1]}
    assert slept == [7.0, 7.0]


def test_it_gives_up_after_the_configured_attempts():
    client, slept = client_for([Response(429)] * 3)

    with pytest.raises(FetchError, match="after 3 attempts"):
        client.get_json("http://x", {}, label="Imbalance")

    assert len(slept) == 2
